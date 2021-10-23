package utilities

import (
	"fmt"
	"strings"
)

func IsValidCreateQueueInput(appName, name string, delaySeconds *uint16, visibilityTimeout *uint16) error {

	if len(strings.TrimSpace(appName)) == 0 || len(strings.TrimSpace(name)) == 0 {
		return &InvalidInputError{appName, name, "One more inputs were empty"}
	}

	if *delaySeconds > 30 {
		*delaySeconds = 0
	}

	if *visibilityTimeout > 30 {
		*visibilityTimeout = 0
	}

	return nil
}

func IsValidMessageInput(appName, name, value string) error {
	if len(strings.TrimSpace(appName)) == 0 || len(strings.TrimSpace(name)) == 0 || len(strings.TrimSpace(value)) == 0 {
		return &InvalidInputError{appName, name, "One more inputs were empty"}
	}

	return nil
}

func GetSubstring(input, leftDel, righttDel string) string {
	if len(input) == 0 {
		return input
	}

	subString := strings.Split(input, leftDel)
	subString = strings.Split(subString[1], righttDel)

	return subString[1]
}

type InvalidInputError struct {
	AppName string //Application name
	Name    string //Queue name
	Message string //messsage that describes what went wrong
}

func (i *InvalidInputError) Error() string {
	return fmt.Sprintf("%v.%v: %v", i.AppName, i.Name, i.Message)
}
