package ytapi

//
//
//   Wrapper functions for the Official Google YT V3 Data Api
//   Used to fetch metadata about YT media
//   Assumes Environment Variable GOOGLE_API_KEY is supplied
//
//   Install this module in GOPATH
//

import (
	"errors"
	"fmt"

	"log"
	"net/http"

	"awsutils"

	"google.golang.org/api/googleapi/transport"
	"google.golang.org/api/youtube/v3"
)

// YtVideoMetaData - a struct to hold Metadata
type YtVideoMetaData struct {
	Description            string
	Video_id               string
	Channel_name           string
	Title                  string
	Tags                   []string
	Channel_id             string
	Thumbnail_url          string
	Category_id            string
	Published_at           string
	Video_url              string
	Default_audio_language string
	Default_language       string
}

const api_url = "https://www.googleapis.com/youtube/v3/"

var developerKey string = awsutils.Getenv("GOOGLE_API_KEY", "")

// GetYTConn -- gets YT Data Api connection, using the Google Developer Key
func GetYTConn() (*youtube.Service, error) {
	client := &http.Client{
		Transport: &transport.APIKey{Key: developerKey},
	}

	fmt.Println("Dev Key", developerKey)

	service, err := youtube.New(client)
	if err != nil {
		log.Fatalf("Error creating new YouTube client: %v", err)
		return nil, err
	}

	return service, nil
}

// LookupVideoDescription - looks up a video Id
func LookupVideoDescription(service *youtube.Service, video_id string) (YtVideoMetaData, error) {

	// Get meta-data for a video """
	var ytMeta YtVideoMetaData

	fmt.Println("Looking up", video_id)

	call := service.Videos.List("snippet,contentDetails,statistics")
	call.Id(video_id)
	response, _ := call.Do()
	fmt.Println(response)

	if len(response.Items) < 1 {
		return YtVideoMetaData{}, fmt.Errorf("Video not found on YT")
	}

	ytMeta.Description = response.Items[0].Snippet.Description
	ytMeta.Video_id = video_id
	ytMeta.Channel_name = response.Items[0].Snippet.ChannelTitle
	ytMeta.Tags = response.Items[0].Snippet.Tags
	ytMeta.Channel_id = response.Items[0].Snippet.ChannelId
	ytMeta.Thumbnail_url = response.Items[0].Snippet.Description
	ytMeta.Category_id = response.Items[0].Snippet.CategoryId
	ytMeta.Published_at = response.Items[0].Snippet.PublishedAt
	ytMeta.Video_url = fmt.Sprintf("http://www.youtube.com/watch?v=%s", video_id)
	ytMeta.Default_audio_language = response.Items[0].Snippet.DefaultAudioLanguage
	ytMeta.Default_language = response.Items[0].Snippet.DefaultLanguage

	return ytMeta, nil

}

// ChannelsListByUsername - this function gets channel details, given a channel userName
func ChannelsListByUsername(service *youtube.Service, part string, forUsername string) string {
	call := service.Channels.List(part)
	call = call.ForUsername(forUsername)
	response, err := call.Do()
	if err != nil {
		fmt.Printf("Error %v", err)
		panic(err)
	}
	fmt.Printf("response %v", response)
	return response.Items[0].Snippet.Title
}

// ChannelsByID retrieves Channel data for a given ID
func ChannelsByID(service *youtube.Service, part string, channelId string) (string, error) {

	call := service.Channels.List(part)
	call = call.Id(channelId)
	response, err := call.Do()
	if err != nil {
		fmt.Println("Error is not nil in ChannelsByID")
		panic(err)
	}
	if response.HTTPStatusCode != 200 {
		fmt.Println("Https StatusCode ", response.HTTPStatusCode)
	}
	if len(response.Items) > 0 {
		return response.Items[0].Snippet.Title, nil
	}
	return "", errors.New("No items returned")
}
