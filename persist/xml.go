package persist

import (
    "encoding/xml"
    "errors"
    "io/ioutil"
    "os"
    "sync"
)

type XMLServerAddrs struct {
    ServerAddr []ServerAddr
}

type XMLConfig struct {
    Servers    XMLServerAddrs
    NewServers XMLServerAddrs
}

type XMLConfigMeta struct {
    FromLogIndex uint64
    ToLogIndex   uint64
    Conf         *XMLConfig
}

type XMLConfigMetas struct {
    ConfigMeta []XMLConfigMeta
}

type LookupKey struct {
    FromLogIndex uint64
    ToLogIndex   uint64
}

type XMLConfigManager struct {
    lock        sync.RWMutex
    FilePath    string
    Indexes     []uint64
    LookupTable map[uint64]*ConfigMeta
}

func NewXMLConfigManager(filePath string) (*XMLConfigManager, error) {
    fileContent, err := ioutil.ReadFile(filePath)
    if err != nil {
        return nil, err
    }

    var xmlMetas XMLConfigMetas
    err = xml.Unmarshal(fileContent, &xmlMetas)
    if err != nil {
        return nil, err
    }

    metaTotal := len(xmlMetas.ConfigMeta)
    if metaTotal == 0 {
        return nil, errors.New("No config")
    }
    metas := make([]*ConfigMeta, 0, metaTotal)
    for _, xmlMeta := range xmlMetas.ConfigMeta {
        meta := &ConfigMeta{
            FromLogIndex: xmlMeta.FromLogIndex,
            ToLogIndex:   xmlMeta.ToLogIndex,
            Conf: &Config{
                Servers:    xmlMeta.Conf.Servers.ServerAddr,
                NewServers: xmlMeta.Conf.NewServers.ServerAddr,
            },
        }
        metas = append(metas, meta)
    }

    indexes := make([]uint64, 0, metaTotal)
    lookupTable := make(map[uint64]*ConfigMeta)
    for _, meta := range metas {
        fromLogIndex := meta.FromLogIndex
        indexes = append(indexes, fromLogIndex)
        lookupTable[fromLogIndex] = meta
    }

    object := &XMLConfigManager{
        FilePath:    filePath,
        Indexes:     indexes,
        LookupTable: lookupTable,
    }
    return object, nil
}

func (self *XMLConfigManager) PushConfig(
    logIndex uint64, conf *Config) error {

    self.lock.Lock()
    defer self.lock.Unlock()

    if logIndex <= self.Indexes[0] {
        return errors.New("index out of bound")
    }

    lastKey := self.Indexes[len(self.Indexes)]
    self.LookupTable[lastKey].ToLogIndex = logIndex - 1
    self.Indexes = append(self.Indexes, logIndex)
    self.LookupTable[logIndex] = &ConfigMeta{
        FromLogIndex: logIndex,
        ToLogIndex:   0,
        Conf:         conf,
    }

    if err := self.persist(
        self.FilePath, self.Indexes, self.LookupTable); err != nil {
        // rollback
        delete(self.LookupTable, logIndex)
        self.Indexes = self.Indexes[:len(self.Indexes)-1]
        self.LookupTable[self.Indexes[len(self.Indexes)-1]].ToLogIndex = 0
        return err
    }
    return nil
}

func (self *XMLConfigManager) LastConfig() (*Config, error) {
    self.lock.RLock()
    defer self.lock.Unlock()

    return self.LookupTable[self.Indexes[len(self.Indexes)-1]].Conf, nil
}

func (self *XMLConfigManager) GetConfig(logIndex uint64) (*Config, error) {
    self.lock.RLock()
    defer self.lock.Unlock()

    metaIndex, err := self.getMetaIndex(logIndex)
    if err != nil {
        return nil, err
    }
    return self.LookupTable[self.Indexes[metaIndex]].Conf, nil
}

func (self *XMLConfigManager) ListAfter(
    logIndex uint64) ([]*ConfigMeta, error) {

    self.lock.Lock()
    defer self.lock.Unlock()
    metaIndex, err := self.getMetaIndex(logIndex)
    if err != nil {
        return nil, err
    }

    total := len(self.Indexes)
    size := total - metaIndex
    metas := make([]*ConfigMeta, 0, size)
    for i := metaIndex; i < total; i++ {
        metas = append(metas, self.LookupTable[self.Indexes[i]])
    }
    return metas, nil
}

func (self *XMLConfigManager) List() ([]*ConfigMeta, error) {
    self.lock.Lock()
    defer self.lock.Unlock()
    metas := make([]*ConfigMeta, 0, len(self.Indexes))
    for _, index := range self.Indexes {
        metas = append(metas, self.LookupTable[index])
    }
    return metas, nil
}

func (self *XMLConfigManager) TruncateBefore(logIndex uint64) error {
    self.lock.Lock()
    defer self.lock.Unlock()

    if logIndex < self.Indexes[0] {
        // nothing to do
        return nil
    }
    metaIndex, err := self.getMetaIndex(logIndex)
    if err != nil {
        return err
    }
    var newIndexes []uint64
    newLookupTable := make(map[uint64]*ConfigMeta)
    if logIndex == self.LookupTable[self.Indexes[metaIndex]].ToLogIndex {
        metaIndex++
        newIndexes = self.Indexes[metaIndex:]
        for k, v := range self.LookupTable {
            if k < self.Indexes[metaIndex] {
                continue
            }
            newLookupTable[k] = v
        }
    } else {
        newIndexes = make([]uint64, 0, len(self.Indexes)-metaIndex)
        newIndexes = append(newIndexes, logIndex)
        newIndexes = append(newIndexes, self.Indexes[metaIndex+1:]...)
        for k, v := range self.LookupTable {
            if k < self.Indexes[metaIndex] {
                continue
            }
            if k == self.Indexes[metaIndex] {
                newLookupTable[logIndex] = v
                newLookupTable[logIndex].FromLogIndex = logIndex
            } else {
                newLookupTable[k] = v
            }

        }
    }

    err = self.persist(self.FilePath, newIndexes, newLookupTable)
    if err != nil {
        return err
    }
    self.Indexes = newIndexes
    self.LookupTable = newLookupTable
    return nil
}

func (self *XMLConfigManager) TruncateAfter(logIndex uint64) error {
    self.lock.Lock()
    defer self.lock.Unlock()
    total := len(self.Indexes)
    if logIndex > self.Indexes[total-1] {
        // nothing to do
        return nil
    }
    metaIndex := 0
    if logIndex >= self.Indexes[0] {
        var err error
        metaIndex, err = self.getMetaIndex(logIndex)
        if err != nil {
            return err
        }
    }
    var newIndexes []uint64
    newLookupTable := make(map[uint64]*ConfigMeta)
    if logIndex == self.LookupTable[self.Indexes[metaIndex]].FromLogIndex {
        if metaIndex != 0 {
            newIndexes = self.Indexes[:metaIndex]
            for k, v := range self.LookupTable {
                if k < self.Indexes[metaIndex] {
                    newLookupTable[k] = v
                }
            }
        }
    } else {
        newIndexes = make([]uint64, 0, metaIndex+1)
        newIndexes = append(newIndexes, self.Indexes[:metaIndex+1]...)
        for k, v := range self.LookupTable {
            if k > self.Indexes[metaIndex] {
                continue
            }
            newLookupTable[k] = v
            if k == self.Indexes[metaIndex] {
                newLookupTable[k].ToLogIndex = 0
            }
        }
    }

    err := self.persist(self.FilePath, newIndexes, newLookupTable)
    if err != nil {
        return err
    }
    self.Indexes = newIndexes
    self.LookupTable = newLookupTable
    return nil
}

func (self *XMLConfigManager) persist(
    filePath string,
    indexes []uint64,
    lookupTable map[uint64]*ConfigMeta) error {

    file, err := os.OpenFile(
        filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
    if err != nil {
        return err
    }
    encoder := xml.NewEncoder(file)

    xmlMetas := &XMLConfigMetas{
        ConfigMeta: make([]XMLConfigMeta, 0, len(indexes)+1),
    }
    for _, fromLogIndex := range indexes {
        meta := lookupTable[fromLogIndex]
        xmlMeta := XMLConfigMeta{
            FromLogIndex: meta.FromLogIndex,
            ToLogIndex:   meta.ToLogIndex,
            Conf: &XMLConfig{
                Servers: XMLServerAddrs{
                    ServerAddr: meta.Conf.Servers,
                },
                NewServers: XMLServerAddrs{
                    ServerAddr: meta.Conf.NewServers,
                },
            },
        }
        xmlMetas.ConfigMeta = append(xmlMetas.ConfigMeta, xmlMeta)
    }

    if err = encoder.Encode(xmlMetas); err != nil {
        return err
    }
    return nil
}

func (self *XMLConfigManager) getMetaIndex(logIndex uint64) (int, error) {
    if logIndex < self.Indexes[0] {
        return 0, errors.New("index out of bound")
    }
    total := len(self.Indexes)
    if logIndex >= self.Indexes[total-1] {
        return total - 1, nil
    }

    beginIndex := 0
    endIndex := total - 1
    for {
        middleIndex := (beginIndex + endIndex) / 2
        if (self.Indexes[middleIndex] <= logIndex) &&
            (self.Indexes[middleIndex+1] > logIndex) {
            return middleIndex, nil
        }
        if logIndex < self.Indexes[middleIndex] {
            endIndex = middleIndex
        } else {
            beginIndex = middleIndex
        }
    }
    return 0, errors.New("fatally no match index")
}
