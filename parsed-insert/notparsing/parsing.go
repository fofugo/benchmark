package notparsing

import (
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
	Content string
}

func GetLogTemplate(log string) (logTemplate LogTemplate, err error) {
	parsedData := map[string]string{}
	if parsedData, err = prepareParsing([]string{TIME, DATA}, log); err != nil {
		return
	}
	logTemplate.Time = parsedData[TIME]
	logTemplate.Content = parsedData[DATA]
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
