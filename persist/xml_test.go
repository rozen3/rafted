package persist

import (
    "testing"
)

func TestXMLConfig(t *testing.T) {
    filePath := "./xml_config.xml"
    metas, err := NewXMLConfigManager(filePath)
    if err != nil {
        t.Error(err)
    }
    for _, meta := range metas {
        t.Log(meta)
        t.Log(meta.Conf)
    }
}
