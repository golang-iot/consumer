package main

import (
	"encoding/gob"
	"github.com/gocql/gocql"
	"log"
	"os"
	//"crypto/md5"
	"time"
	"github.com/golang-iot/queue"
	"github.com/golang-iot/aws"
	"path/filepath"
	"math/rand"
	"github.com/joho/godotenv"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func init() {
	log.Printf("Initializing consumer...")
	gob.Register(queue.Message{})
}

func saveMessage(m queue.Message, session *gocql.Session){
	err := session.Query("INSERT INTO Messages (address, Message, value, created) VALUES (?, ?, ?, ?)", m.Address, m.Message, m.Value, m.Created).Exec()
	if err != nil {
		log.Fatal(err)
	}
}

func saveFaces(device int64, faces []aws.Face, session *gocql.Session){
	if len(faces) > 0{
		f := faces[0]
		err := session.Query("INSERT INTO demo.Faces (device, maxAge, minAge, gender, genderConf, smile, smileConf, emotions, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
														device, f.MaxAge, f.MinAge, f.Gender, f.GenderConf, f.Smile, f.SmileConf, f.Emotions, f.Created).Exec()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	log.Printf("Connecting to RabbitMQ: "+os.Getenv("RABBITMQ_HOST"))
	
	deviceId := int64(1)
	que := queue.Queue{}
	que.Init(os.Getenv("RABBITMQ_HOST"))
	defer que.Close()
	
	que.GetQueue("hello")
	que.GetQueue("images")
	que.GetQueue("fileComplete")
	
	msgs := que.Consume("hello")
	chunks := que.Consume("images")
	
	log.Printf("Connecting to Cassandra: "+os.Getenv("CASSANDRA_HOST"))
	
	cluster := gocql.NewCluster(os.Getenv("CASSANDRA_HOST"))
	cluster.Keyspace = "demo"
	
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: os.Getenv("CASSANDRA_USER"),
		Password: os.Getenv("CASSANDRA_PASSWORD"),
	}
	
	cluster.Consistency = gocql.LocalOne
	session, err := cluster.CreateSession()
	failOnError(err, "Could not connect to Cassandra")
	
	s3 := aws.S3Manager{}
	s3.Init(os.Getenv("AWS_ACCESS_KEY_ID"),os.Getenv("AWS_SECRET_ACCESS_KEY"),"",os.Getenv("AWS_REGION"))
	
	
	go func(){
		//fileList := make(map[string]*os.File)
		path := filepath.Clean(os.Getenv("IMGS_PATH"));
		//os.MkdirAll(path,0777)
		/*
		if err != nil {
			failOnError(err, "could not create folder")
		}*/
	
		chunkCount := make(map[string]int)
	
		for fc := range chunks {
			log.Printf("Chunk")
			m := queue.ChunkFromGOB64(string(fc.Body))
			//log.Printf("File chunk for %s: %d of %d: %s", m.Name, m.Current, m.Total, md5.Sum(m.Content))
			
			
			if val, ok := chunkCount[m.Name]; ok {
				chunkCount[m.Name] = val + 1
			} else {
				chunkCount[m.Name] = 1
			}
			
			filename := filepath.Join(path, string(m.Id+"-"+m.Name))
			
			file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
				failOnError(err, "could not open file")
			
			go func(){
				_, err = file.Write(m.Content)
				failOnError(err, "could not write to file")
			}()
			
			defer file.Close()
			
			if m.Total == m.Current {
				log.Printf("Saved %d chunks out of %d\n", chunkCount[m.Name], m.Total)
			
				
				
				/*
				err := s3.Put(filename, "/faces", "golang-iot")
				if err != nil{
					log.Printf("Could not upload to S3: %s",err)
				}
				*/
				go func(){
					faces, err := s3.SendToRekognition(filename)
					if err != nil{
						log.Printf("Could not upload to S3: %s",err)
					}
					saveFaces(deviceId, faces, session)
				}()
				
				rand.Seed(time.Now().UTC().UnixNano())
					
				me := new(queue.Message)
				me.Message = "File Received:"+m.Name
				me.Address = rand.Intn(1000)
				me.Created = time.Now()
				
				body := queue.ToGOB64(*me)
				
				err = que.Send("fileComplete", body)

				failOnError(err, "Failed to publish a message")
			}
		}
		
	}()
	
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			m := queue.FromGOB64(string(d.Body))
			//log.Printf("queue.Message: %v\n", m)
			saveMessage(m, session)
		}
	}()

	log.Printf(" Consumer initialized")
	log.Printf(" [*] Waiting for queue.Messages. To exit press CTRL+C")
	<-forever
}
