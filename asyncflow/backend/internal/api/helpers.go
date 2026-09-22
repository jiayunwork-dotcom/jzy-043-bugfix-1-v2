package api

import "encoding/json"

// jsonMarshal normalizes the loosely-typed request payload to JSON bytes.
func jsonMarshal(v any) ([]byte, error) {
	if v == nil {
		return []byte(`{}`), nil
	}
	switch x := v.(type) {
	case []byte:
		return x, nil
	case json.RawMessage:
		return x, nil
	case string:
		return []byte(x), nil
	default:
		return json.Marshal(v)
	}
}
