using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using System.Collections.Generic;
using System.Text;
using System.Linq.Dynamic.Core;
using CsvHelper;
using System.Globalization;
using System.Linq;
using Parquet;

namespace ReadAndFilterDatalakeFilesData
{
    public static class ReadAndFilterDatalakeFilesData
    {
        public static string datalakeConnectionString = "";
        [FunctionName("ReadAndFilterDatalakeFilesData")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processing a request.");
            try
            {
                string filePath = req.Query["filePath"];
                string outputFileFormat = req.Query["outputFileFormat"];
                List<StudentData> allData = new List<StudentData>();
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                dynamic body = JsonConvert.DeserializeObject(requestBody);
                filePath = filePath ?? body?.filePath;
                outputFileFormat = outputFileFormat ?? body?.outputFileFormat;
                string filterQuery = body?.filterQuery;
                var storageAccount = new DataLakeServiceClient(datalakeConnectionString);
                string[] strPath = filePath.Split("/");
                string container = string.Empty;
                string newFoldername = string.Empty;

                DataLakeFileSystemClient dirClient = storageAccount.GetFileSystemClient(strPath[0]);
                if (strPath.Length == 1) //Container inside file "ContainerName/test1.txt"
                {
                    var enumerator = dirClient.GetPathsAsync().GetAsyncEnumerator();
                    await ReadAllFileContent(allData, dirClient, enumerator);

                }
                else
                {
                    DataLakeDirectoryClient directory = dirClient.GetDirectoryClient(strPath[1]);
                    if (strPath.Length == 2) // Container inside folder file "ContainerName/folder1/test1.txt"
                    {
                        var enumerator = directory.GetPathsAsync().GetAsyncEnumerator();
                        await ReadAllFileContent(allData, dirClient, enumerator);
                    }
                    else
                    {
                        //Multi level datalake folder file "ContainerName/folder1/folder2/folder3/test1.txt"
                        int i = 0;
                        foreach (string str in strPath)
                        {
                            i++;
                            if (i > 2 && i < strPath.Length)
                            {
                                container = container + str + ((i + 1 == strPath.Length) ? "" : "/");
                            }
                            if (i == strPath.Length)
                                newFoldername = str;
                        }
                        DataLakeDirectoryClient subdirectory = directory.GetSubDirectoryClient(container);

                        var enumerator = subdirectory.GetPathsAsync().GetAsyncEnumerator();
                        await ReadAllFileContent(allData, dirClient, enumerator);

                    }
                }

                List<StudentData> filteredRecords = new List<StudentData>();
                filteredRecords = ApplyFilters(filterQuery, allData);

                if (outputFileFormat == "csv")
                {
                    return SerializeToCsv(filteredRecords);

                }
                else if (outputFileFormat == "json")
                {
                    string jsonString = System.Text.Json.JsonSerializer.Serialize(filteredRecords);
                    //return new OkObjectResult(jsonString);
                    return new FileContentResult(Encoding.UTF8.GetBytes(jsonString), "application/octaet-stream")
                    {
                        FileDownloadName = "result.json"
                    };
                }
                else if (outputFileFormat == "parquet")
                {
                    using (var mem = new MemoryStream())
                    {
                        await ParquetConvert.SerializeAsync(filteredRecords, mem);
                        var parquetData = mem.ToArray();
                        mem.Position = 0;
                        StreamReader R = new StreamReader(mem, true);
                        return new FileContentResult(parquetData, "application/octaet-stream")
                        {
                            FileDownloadName = "result.parquet"
                        };
                    }
                }
                //this commented approach also works but its an overhead to create schema and then serialize it. Keeping it here for reference
                //         var deviceId = new DataColumn(
                //       new DataField<string>("deviceId"),
                //       filteredRecords.Select(x => x.deviceId).ToArray()
                //   );
                //         var eventDate = new DataColumn(
                //    new DataField<string>("eventDate"),
                //    filteredRecords.Select(x => x.eventDate).ToArray());
                //         var temperature = new DataColumn(

                //             new DataField<int>("temperature"),
                //    filteredRecords.Select(x => x.temperature).ToArray());
                //         var humidity = new DataColumn(
                //    new DataField<int>("humidity"),
                //    filteredRecords.Select(x => x.humidity).ToArray()
                //);
                //         var co2 = new DataColumn(
                //    new DataField<int>("co2"),
                //    filteredRecords.Select(x => x.co2).ToArray()
                //);

                // var schema = new Schema(deviceId.Field, eventDate.Field, temperature.Field, humidity.Field, co2.Field);



                //         //string jsonString = ParquetConvert.SerializeAsync(schema, mem, filteredRecords);
                //         using (var mem = new MemoryStream())
                //         {
                //             using (var parquetWriter = await ParquetWriter.CreateAsync(schema, mem))
                //             {
                //                 // create a new row group in the file
                //                 using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                //                 {
                //                     await groupWriter.WriteColumnAsync(deviceId);
                //                     await groupWriter.WriteColumnAsync(eventDate);
                //                     await groupWriter.WriteColumnAsync(temperature);
                //                     await groupWriter.WriteColumnAsync(humidity);
                //                     await groupWriter.WriteColumnAsync(co2);

                //                 }
                //             }
                //             mem.Position = 0;
                //             StreamReader R = new StreamReader(mem);
                //             var result = R.ReadToEnd();
                //             File.WriteAllText(@"D://environment_history_3.parquet", result);
                //             return new OkObjectResult(result);

                //         }

                //}
                else
                    return new OkObjectResult("invalid format");

            }
            catch (Exception ex)
            {
                return new BadRequestObjectResult(ex.Message);
            }
        }

        private async static Task<List<StudentData>> DeserializeParquetData(FileDownloadInfo data)
        {
            var ms = new MemoryStream();

            data.Content.CopyTo(ms);
            ms.Position = 0;
            var records = await ParquetConvert.DeserializeAsync<StudentData>(ms);
            return records.ToList();
        }

        private static async
        Task
        ReadAllFileContent(List<StudentData> allData, DataLakeFileSystemClient dirClient, IAsyncEnumerator<PathItem> enumerator)
        {
            await enumerator.MoveNextAsync();
            PathItem item = enumerator.Current;

            while (item != null)
            {
                Console.WriteLine(item.Name);
                DataLakeFileClient fileClient = dirClient.GetFileClient(item.Name);
                var data = await fileClient.ReadAsync();
                if (item.Name.EndsWith(".csv"))
                {
                    var records = DeserializeCsvData(data);
                    allData.AddRange(records);
                }
                else if (item.Name.EndsWith(".json"))
                {
                    var records = DeserializeJsonData(data);
                    allData.AddRange(records);
                }
                else if (item.Name.EndsWith(".parquet"))
                {
                    var records = DeserializeParquetData(data);
                    allData.AddRange(records.Result);
                }

                if (!await enumerator.MoveNextAsync())
                {
                    break;
                }
                item = enumerator.Current;
            }

        }

        private static List<StudentData> DeserializeJsonData(FileDownloadInfo data)
        {
            return System.Text.Json.JsonSerializer.Deserialize<StudentData[]>(data.Content).ToList();
        }

        private static List<StudentData> DeserializeCsvData(FileDownloadInfo data)
        {
            var reader = new StreamReader(data.Content);
            var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

            var records = csv.GetRecords<StudentData>();

            return records.ToList();
        }

        private static IActionResult SerializeToCsv(List<StudentData> filteredRecords)
        {
            using (var mem = new MemoryStream())
            using (var writer = new StreamWriter(mem))
            using (var csvWr = new CsvWriter(writer, CultureInfo.CurrentCulture, false))
            {
                //csvWr.Configuration.HasHeaderRecord = false;
                //csvWr.Configuration.Delimiter = ',';
                csvWr.WriteRecords(filteredRecords);

                writer.Flush();
                var result = mem.ToArray();

                //return new OkObjectResult(result);
                return new FileContentResult(result, "application/octaet-stream")
                {
                    FileDownloadName = "result.csv"
                };
            }
        }

        private static List<StudentData> ApplyFilters(string filterQuery, List<StudentData> records)
        {
            if (!string.IsNullOrEmpty(filterQuery))
            {
                var filter = "x => " + filterQuery;
                return records.AsQueryable().Where(filter).ToList();
            }
            else return records;
        }
    }

    public class StudentData
    {
        public string Student_ID { get; set; }
        public string Student_Name { get; set; }
        public int Studentt_Age { get; set; }
        public string Student_DOB { get; set; }
        public string Student_Place { get; set; }
        public string Student_DOJ { get; set; }
        public string Student_Gender { get; set; }

    }
}
