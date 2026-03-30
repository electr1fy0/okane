package utils

func NullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}
