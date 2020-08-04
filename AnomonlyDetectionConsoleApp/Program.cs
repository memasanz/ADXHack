using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;


namespace AnomonlyDetectionConsoleApp
{
    public class Program
    {

        private static EventHubClient eventHubClient;
        private static string EventHubConnectionString = "Endpoint=sb://chr-events.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=InHZJx33zenwCVGqHwvyPqHB942wSRfFzDlIST4tfDA=";
        private static string EventHubName = "chr-events";
        private static string SourceFileLocation = "C:\\Users\\memasanz\\Downloads\\6-18\\6-18.csv";



        public static void Main(string[] args)
        {

            // Type your username and press enter
            Console.WriteLine("Enter line number:");

            // Create a string variable and get user input from the keyboard and store it in the variable
            

            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            int inputLineNumber = Convert.ToInt32(Console.ReadLine());
          

            // Creates an EventHubsConnectionStringBuilder object from the connection string, and sets the EntityPath.
            // Typically, the connection string should have the entity path in it, but for the sake of this simple scenario
            // we are using the connection string from the namespace.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
            var MessageIndex = inputLineNumber;

            var maxRecordCount = getRowCountCSV(SourceFileLocation);

            await SendMessagesToEventHub(MessageIndex, maxRecordCount);

            await eventHubClient.CloseAsync();

            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }

        // Uses the event hub client to send 100 messages to the event hub.
        private static async Task SendMessagesToEventHub(int messageIndex, int numMessagesToSend)
        {

           
            

            for (var i = messageIndex; i < messageIndex + numMessagesToSend; i++)
            {
                try
                {
                    string tempFile = SourceFileLocation;

                    string recordString = ReadCSV(tempFile, i + 1);

                    EventData eventData = new EventData(Encoding.UTF8.GetBytes(recordString));

                    var message = $"Message {i}";
                    Console.WriteLine($"Sending message: {recordString}");
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(recordString)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                //await Task.Delay(10);
            }

            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }

        private static int getRowCountCSV(string filePath)
        {
            return File.ReadAllLines(filePath).Length;
        }
        private static string ReadCSV(string filePath, int actualLineNumber)
        {
            
            string tempData =  File.ReadLines(filePath).ElementAt(actualLineNumber - 1);
            CHRMessage chrMessage = FromCsv(tempData);
            string message = JsonConvert.SerializeObject(chrMessage);
            return message;
        }

        public static CHRMessage FromCsv(string csvLine)
        {
            string[] values = csvLine.Split(',');
            if (values[5] == "NULL")
                values[5] = null;

            CHRMessage chrMessage = new CHRMessage()
            {
                processed = Convert.ToDateTime(values[0]),
                transactionType = values[1],
                direction = values[2],
                partner = values[3],
                serverClusterMainNode = values[4],
                errorResolutionType = Convert.ToInt32(values[5]),
                purpose = values[6],
                loadNum = values[7],
                shipmentNum = values[8],
                proNum = values[9]
            };
            return chrMessage;
        }

    }
}
