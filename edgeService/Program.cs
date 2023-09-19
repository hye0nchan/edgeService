﻿using FirebaseAdmin;
using FirebaseAdmin.Messaging;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Firestore;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO.Ports;
using System.Linq;
using System.Net;
using System.Reactive;
using System.Text;
using System.Timers;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace EdgeService
{

    public class Firebase
    {
        public FirestoreDb? db { get; set; }
        public List<String>? token { get; set; }
        
        //firebase 를 설정하는 코드
        public void initialzation()
        {
            token = new List<string>();
            string path = AppDomain.CurrentDomain.BaseDirectory + @"newwayfarm-74ecc-firebase-adminsdk-2218b-71563d557f.json";
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", path);
            db = FirestoreDb.Create("newwayfarm-74ecc");
        }

        //firebase의 토큰을 읽는 코드
        public async Task readToken()
        {
            try
            {
                if (db != null)
                {
                    CollectionReference usersRef = db.Collection("Users");
                    QuerySnapshot snapshot = await usersRef.GetSnapshotAsync();
                    foreach (DocumentSnapshot document in snapshot.Documents)
                    {
                        if (document.Exists)
                        {
                            Console.WriteLine("Document ID: " + document.Id);

                            if (document.ContainsField("fcmToken"))
                            {
                                var fcmToken = document.GetValue<string>("fcmToken");
                                if (token != null)
                                {
                                    token.Add(fcmToken);
                                }
                            }

                        }
                    }
                }     
            }
            catch (Exception _ex)
            {
                System.Console.WriteLine(_ex);
            }
        }

        public async Task sendNotification()
        //fcm test, 서버키 없이 가능
        {
            FirebaseApp.Create(new AppOptions()
            {
                Credential = GoogleCredential.FromFile("newwayfarm-74ecc-firebase-adminsdk-2218b-71563d557f.json")
            });

            if (token != null)
            {
                var message = new Message()
                {
                    Token = token[0],
                    Notification = new FirebaseAdmin.Messaging.Notification
                    {
                        Title = "Title",
                        Body = "Message body"
                    }
                };
                // Send the message
                string response = await FirebaseMessaging.DefaultInstance.SendAsync(message);
            }

            
        }
    }
    class Program
    {
        static StringBuilder jsonBuffer = new StringBuilder();
        private static Dictionary<int, JObject> receivedJsonObjects = new Dictionary<int, JObject>();

        //MQTT 변수 선언
        public static uPLibrary.Networking.M2Mqtt.MqttClient? client;
        public static string? clientId; // client id 

        static SerialPort mySerialPort = new SerialPort("/dev/ttyAMA1");
        public static string serialNumber = "";
        private static int retryCount = 0;
        private static bool dataReceived = false;
        private static bool blockSerialData = false;

        //influxDB 변수 선언
        public static InfluxDBClient influxDBClient = new InfluxDBClient("http://saltware.mooo.com:6001", "PQebYVWrTYe4cqe0MetS90XLe80AQUxYqRKbNNtOrrFDg2UO2HPua5u8atv0JdaR_m38hSC0FPYiEFUBnOaviQ==");



        private static bool HasDuplicateSensorId(JArray sensors)
        {
            HashSet<int> uniqueSensorIds = new HashSet<int>();
            foreach (var sensor in sensors)
            {
                int? sensorId = (int?)sensor["sensor_id"];
                if (sensorId.HasValue)
                {
                    if (uniqueSensorIds.Contains(sensorId.Value))
                    {
                        return true;
                    }
                    uniqueSensorIds.Add(sensorId.Value);
                }
                
            }
            return false;
        }

        private static void DataReceivedHandler(object sender, SerialDataReceivedEventArgs e)
        {
            if (blockSerialData)
            {
                return;
            }
            SerialPort sp = (SerialPort)sender;
           
            string indata = sp.ReadExisting();

            if (string.IsNullOrEmpty(indata) || indata.Contains("error"))
            {
                Console.Write("end");
                return;
            }

            jsonBuffer.Append(indata);

            int bracketCount = 0;
            int startIdx = -1;
            int endIdx = -1;

            for (int i = 0; i < jsonBuffer.Length; i++)
            {
                if (jsonBuffer[i] == '{')
                {
                    bracketCount++;
                    if (startIdx == -1) startIdx = i;
                }
                else if (jsonBuffer[i] == '}')
                {
                    bracketCount--;
                    if (bracketCount == 0)
                    {
                        endIdx = i;
                        break;
                    }
                }
            }

            if (startIdx != -1 && endIdx != -1)
            {
                string completeJson = jsonBuffer.ToString().Substring(startIdx, endIdx - startIdx + 1);
                jsonBuffer.Remove(0, endIdx + 1); // clear the processed part
                jsonBuffer.Clear();

                try
                {
                    JObject jObject = JObject.Parse(completeJson);
                    int? id = (int?)jObject["id"];

                    if (id.HasValue)
                    {
                        receivedJsonObjects[id.Value] = jObject;
                    }
                    

                    if (receivedJsonObjects.Count >= 2)
                    {
                        MergeJsonObjects();
                        dataReceived = true;
                        receivedJsonObjects.Clear();
                    }
                }
                catch (JsonReaderException jsonEx)
                {
                    Console.WriteLine("JSON Parsing Error: " + jsonEx.Message);
                }
            }
        }
        private static async void MergeJsonObjects()
        {
            if (receivedJsonObjects == null)
            {
                return;
            }

            JArray finalArray = new JArray();  // 최종 JSON 배열

            try
            {
                foreach (var pair in receivedJsonObjects)
                {
                    if (pair.Value != null)
                    {
                        JObject device = pair.Value;
                        JArray? sensors = (JArray?)device["sensors"];

                        if(sensors != null)
                        {
                            foreach (JObject sensor in sensors)
                            {
                                // value 필드를 가져옵니다.
                                JToken? valueToken = sensor["value"];

                                if (valueToken != null && valueToken.Type == JTokenType.String)
                                {
                                    // 공백을 제거하고 float로 변환
                                    string valueStr = valueToken.ToString().Trim();
                                    if (float.TryParse(valueStr, out float valueFloat))
                                    {
                                        // 변환에 성공하면 원래 필드를 업데이트
                                        sensor["value"] = valueFloat;
                                    }
                                }
                            }
                            finalArray.Add(pair.Value);  // 수신된 각 JSON 객체를 최종 배열에 추가
                        }

                        
                    }
                    else
                    {
                        return;
                    }

                }
            }
            
            catch(Exception ex)
            {
                Console.WriteLine("json Error: ",ex);
                return;
            }
            var sortedFinalArray = new JArray(finalArray.OrderBy(obj => (int?)obj["id"]));

            string finalJsonString = sortedFinalArray.ToString();  // 최종 JSON 문자열

            Console.WriteLine("Merged JSON:");
            Console.WriteLine(finalJsonString);

            var influxData = new List<PointData>();

            try
            {
                foreach (var device in sortedFinalArray)
                {
                    int? id = (int?)device["id"];
                  
                    JArray? sensors = (JArray?)device["sensors"];

                    if (sensors != null)
                    {
                        foreach (JToken sensor in sensors)
                        {
                            int? sensorId = (int?)sensor["sensor_id"];
                            double? value = (double?)sensor["value"];

                            // InfluxDB PointData 생성
                            var point = PointData.Measurement($"{id}-{sensorId}")
                                                  .Tag("serialNumber", "2309013Fg7")
                                                  .Field("value", value)
                                                  .Timestamp(DateTime.UtcNow, WritePrecision.Ns);

                            influxData.Add(point);
                        }
                    }
                    
                }
                var writeApi = influxDBClient.GetWriteApiAsync();
                await writeApi.WritePointsAsync(influxData, "2309013Fg7", "saltware");  // org-id를 실제 조직 ID로 대체해야 합니다.

                // MQTT로 데이터 전송
                if (client != null){
                    client.Publish("saltware/newwayFarm/test/2309013Fg7/receive/sensor", Encoding.UTF8.GetBytes(finalJsonString), MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE, false);
                    blockSerialData = true;
                }
               
            }

            catch(Exception ex)
            {
                Console.WriteLine("inlfux Error: ", ex);
                return;
            }
            
        }
       
        static void Main(string[] args)
        {
            System.Timers.Timer timer = new System.Timers.Timer();
            timer.Interval = 30000;
            timer.Elapsed += new ElapsedEventHandler(timer_Elapsed);
            timer.Start();

            string brokerAddress = "saltware.mooo.com";

            int maxMqttRetries = 5;
            int delay = 2000; // 2 seconds
            for (int i = 0; i < maxMqttRetries; i++)
            {
                try
                {
                    client = new MqttClient(brokerAddress, 1884, false, null, null, MqttSslProtocols.None);
                    clientId = Guid.NewGuid().ToString();
                    client.Connect(clientId);
                    client.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;
                    serialNumber = "2309013Fg7";
                    break; // Exit the loop if successful
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Retry {i + 1}: {ex.Message}");
                    Task.Delay(delay).Wait(); // Wait before next retry
                    delay *= 2; // Exponential backoff
                }
            }


            // 시리얼 포트 설정sks 
            //SerialPort mySerialPort = new SerialPort("/dev/ttyAMA1");

            mySerialPort.BaudRate = 9600;
            mySerialPort.Parity = Parity.None;
            mySerialPort.StopBits = StopBits.One;
            mySerialPort.DataBits = 8;
            mySerialPort.Handshake = Handshake.None;

            // 데이터가 도착했을 때의 이벤트 핸들러 설정
            mySerialPort.DataReceived += new SerialDataReceivedEventHandler(DataReceivedHandler);

            //시리얼넘버 구독
            if(client != null)
            {
                client.Subscribe(new string[] { "saltware/newwayFarm/test/2309013Fg7/request/sensor" }, new byte[] { MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE });
            }
            
        }

        static async void timer_Elapsed(object? sender, ElapsedEventArgs e)
        {
            await SendLoRaAsync("sensor");
        }

        


        private static async Task SendLoRaAsync(string message)
        {
            try
            {
                // 시리얼 포트 열기
                if (!mySerialPort.IsOpen)
                {
                    mySerialPort.Open();
                }
                
            }
            catch (Exception ex)
            {
                Console.WriteLine("error" + ex.ToString());
                return;
            }

            if(!string.IsNullOrEmpty(message))
            {
                // 최대 3번까지만 시도
                if (retryCount < 3)
                {
                    if (!dataReceived)
                    {
                        mySerialPort.WriteLine("sensor0" + '\n');
                        await Task.Delay(500);
                        mySerialPort.WriteLine("sensor1" + '\n');
                    }      
                    await Task.Delay(1500);

                    if (!dataReceived)
                    {
                        retryCount++;  // 재시도 횟수 증가
                        await SendLoRaAsync(message);
                    }
                    else
                    {
                        retryCount = 0;
                        dataReceived = false;
                    }
                }
                else
                {
                    Console.WriteLine("receive error.");
                    retryCount = 0;
                    dataReceived = false;
                }

            }
           
        }

   
        private static async void Client_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
        {
            // 어떤 요청인지 확인하는 코드
            string receivedMessage = Encoding.UTF8.GetString(e.Message);
            string receivedTopic = e.Topic.ToString();
            Console.WriteLine(receivedMessage);
            Console.WriteLine(receivedTopic);
            

            if (receivedTopic == "saltware/newwayFarm/test/2309013Fg7/request/sensor")
            {
                if (receivedMessage.Contains("sensor"))
                {
                    blockSerialData = false;
                    //lora 신호 전송 코드
                    await SendLoRaAsync(receivedMessage);
                    Console.WriteLine("send complete");
                }
            }

        }

    }
}
