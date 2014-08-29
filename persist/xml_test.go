package persist

import (
    "testing"
)

func TestXMLConfig(t *testing.T) {
    filePath := "./xml_config.xml"
    manager, err := NewXMLConfigManager(filePath)
    if err != nil {
        t.Error(err)
    }
    for _, meta := range manager.LookupTable {
        t.Log(meta)
        t.Log(meta.Conf)
    }
}
