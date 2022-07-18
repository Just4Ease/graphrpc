package server

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"errors"
	"example/server/generated"
	"example/server/model"
	"fmt"
	"os"
	"strconv"

	"github.com/99designs/gqlgen/graphql"
)

func (r *mutationResolver) Post(ctx context.Context, input model.PostCreateInput) (*model.Message, error) {
	return &model.Message{
		Text: input.Text,
	}, nil
}

func (r *mutationResolver) UploadFile(ctx context.Context, file graphql.Upload) (*model.UploadData, error) {
	return &model.UploadData{
		Size: int(file.Size),
	}, nil
}

func (r *mutationResolver) UploadFiles(ctx context.Context, files []*graphql.Upload) ([]*model.UploadData, error) {
	ds := make([]*model.UploadData, 0)
	for _, f := range files {
		ds = append(ds, &model.UploadData{
			Size: int(f.Size),
		})
	}
	return ds, nil
}

func (r *mutationResolver) UploadFilesMap(ctx context.Context, files model.UploadFilesMapInput) (*model.UploadFilesMap, error) {
	return &model.UploadFilesMap{
		Somefile: &model.UploadData{
			Size: int(files.Somefile.Size),
		},
	}, nil
}

func (r *queryResolver) Room(ctx context.Context, name string) (*model.Chatroom, error) {
	if name == "error" {
		return nil, errors.New("that's an invalid room")
	}

	return &model.Chatroom{
		Name:     name,
		Messages: nil,
	}, nil
}

func (r *queryResolver) RoomNonNull(ctx context.Context, name string) (*model.Chatroom, error) {
	return r.Room(ctx, name)
}

func (r *queryResolver) Medias(ctx context.Context) ([]model.Media, error) {
	return []model.Media{
		&model.Image{
			Size: 100,
		},
		&model.Video{
			Duration: 200,
		},
	}, nil
}

func (r *queryResolver) Books(ctx context.Context) ([]model.Book, error) {
	return []model.Book{
		&model.Textbook{
			Title:   "Some textbook",
			Courses: []string{"course 1", "course 2"},
		},
		&model.ColoringBook{
			Title:  "Some Coloring Book",
			Colors: []string{"red", "blue"},
		},
	}, nil
}

func (r *queryResolver) Issue8(ctx context.Context) (*model.Issue8Payload, error) {
	return &model.Issue8Payload{
		Foo1: &model.Issue8PayloadFoo{A: &model.Issue8PayloadFooA{Aa: "foo1"}},
		Foo2: &model.Issue8PayloadFoo{A: &model.Issue8PayloadFooA{Aa: "foo2"}},
	}, nil
}

func (r *queryResolver) Cyclic(ctx context.Context) (*model.Cyclic1_1, error) {
	panic(fmt.Errorf("not implemented"))
}

func (r *queryResolver) Episodes(ctx context.Context) ([]model.Episode, error) {
	return []model.Episode{
		model.EpisodeJedi,
		model.EpisodeNewhope,
		model.EpisodeEmpire,
	}, nil
}

func (r *queryResolver) AsMap(ctx context.Context, req map[string]interface{}, opt map[string]interface{}) (string, error) {
	return fmt.Sprintf("req: %+v opt: %+v", req, opt), nil
}

func (r *queryResolver) OptValue1(ctx context.Context, req model.OptionalValue1) (*bool, error) {
	panic(fmt.Errorf("not implemented"))
}

func (r *queryResolver) OptValue2(ctx context.Context, opt *model.OptionalValue2) (*bool, error) {
	panic(fmt.Errorf("not implemented"))
}

func (r *subscriptionResolver) MessageAdded(ctx context.Context, roomName string) (<-chan *model.Message, error) {
	ch := make(chan *model.Message)
	debug, _ := strconv.ParseBool(os.Getenv("GQLGENC_WS_LOG"))

	debugPrint := func(a ...interface{}) {
		if debug {
			fmt.Println(a...)
		}
	}

	debugPrint("MESSAGE ADDED")

	go func() {
		i := 0
		for {
			if i == 3 {
				close(ch)
				debugPrint("DONE MESSAGE ADDED")
				return
			}

			msg := &model.Message{
				ID: fmt.Sprintf("msg%v", i),
			}

			select {
			case <-ctx.Done():
				close(ch)
				debugPrint("DONE ctx")
				return
			case ch <- msg:
				debugPrint("SEND")
				i++
			}
		}
	}()

	return ch, nil
}

// Mutation returns generated.MutationResolver implementation.
func (r *Resolver) Mutation() generated.MutationResolver { return &mutationResolver{r} }

// Query returns generated.QueryResolver implementation.
func (r *Resolver) Query() generated.QueryResolver { return &queryResolver{r} }

// Subscription returns generated.SubscriptionResolver implementation.
func (r *Resolver) Subscription() generated.SubscriptionResolver { return &subscriptionResolver{r} }

type mutationResolver struct{ *Resolver }
type queryResolver struct{ *Resolver }
type subscriptionResolver struct{ *Resolver }
