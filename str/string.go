package str

import (
    "math/rand"
    "strconv"
    "strings"
    "time"
)

const (
    ASCIILowercase string = "abcdefghijklmnopqrstuvwxyz"
    ASCIIUppercase        = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    ASCIILetters          = ASCIILowercase + ASCIIUppercase
    Digits                = "0123456789"
    OctDigits             = "01234567"
    Punctuation           = "!\"#$%&\\'()*+,-./:;<=>?@[\\]^_`{|}~"
    Printable             = Digits + ASCIILetters + Punctuation + " \t\n\r\x0b\x0c"
)

func RandomString(length uint32) string {
    chars := make([]byte, 0, length)
    rand.Seed(time.Now().UTC().UnixNano())
    for i := uint32(0); i < length; i++ {
        chars = append(chars, ASCIILetters[rand.Intn(len(ASCIILetters))])
    }
    return string(chars)
}

func RandomIP() string {
    fieldSize := 4
    fields := make([]string, fieldSize)
    rand.Seed(time.Now().UTC().UnixNano())
    for i := 0; i < fieldSize; i++ {
        fields = append(fields, strconv.Itoa(rand.Intn(256)))
    }
    separator := ""
    return strings.Join(fields, separator)
}
