package parsing

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
)

const (
	TIME = "Time"
	DATA = "Data"
)

//web에 보여줄 한 row의 struct 입니다.
type LogTemplate struct {
	Time    string
	Head    Head    `json:"head"`
	Content Content `json:"content"`
}

type Head struct {
	Id string `json:"id"`
}

type Content struct {
	Type    json.Number `json:"type"`
	Person  string      `json:"person"`
	Number  string      `json:"number"`
	Age     json.Number `json:"age"`
	Country string      `json:"country"`
}

func GetLogTemplate(log string) (logTemplate LogTemplate, err error) {
	parsedData := map[string]string{}
	if parsedData, err = prepareParsing([]string{TIME, DATA}, log); err != nil {
		return
	}
	json.Unmarshal([]byte(parsedData[DATA]), &logTemplate)
	logTemplate.Time = parsedData[TIME]
	return
}

//npush or mqtt에서 parsing할 부분을 일차적으로 잘라냅니다.
func prepareParsing(keys []string, content string) (results map[string]string, err error) {
	results = map[string]string{}
	var regexpMap = map[string]string{
		TIME: `\A\[[0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}\]`,
		DATA: `Data:(.)+\z`,
	}

	var customTrimMap = map[string][2]string{
		TIME: {`[`, `]`},
		DATA: {`Data:`, ``},
	}
	for _, v := range keys {
		s := ""
		s, err = doRegexp(regexpMap[v], content, true)
		s = strings.TrimPrefix(s, customTrimMap[v][0])
		s = strings.TrimSuffix(s, customTrimMap[v][1])
		if s == "" {
			return
		}
		results[v] = s
	}
	return
}

func doRegexp(regexpValue string, target string, startPosition bool) (result string, err error) {
	re := regexp.MustCompile(regexpValue)
	results := re.FindAllString(target, -1)

	if len(results) == 0 {
		err = errors.New("식별할 수 없는 값이 있습니다.")
		return
	}
	if startPosition == true {
		result = results[0]
	} else {
		result = results[len(results)-1]
	}
	return
}
