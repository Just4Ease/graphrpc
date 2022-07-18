package somelib

import "encoding/json"

type CustomRoom string

func (b *CustomRoom) UnmarshalJSON(data []byte) error {
	var v struct {
		Room struct {
			Name string `json:"name"`
		} `json:"room"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	*b = CustomRoom("Room: " + v.Room.Name)

	return nil
}

func (b CustomRoom) String() string {
	return string(b)
}
