package example

const RoomQuery = `
query query($name: String!) {
	room(name: $name) {
		name
	}
}
`

type RoomQueryResponse struct {
	Room struct {
		Name string `json:"name"`
	} `json:"room"`
}

const MessagesSub = `
subscription query{
	messageAdded(roomName: "test") {
		id
	}
}
`

type MessagesSubResponse struct {
	MessageAdded struct {
		ID string `json:"id"`
	} `json:"messageAdded"`
}
