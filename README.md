# Read And Filter Datalake Files Data

This azure function reads multiple files from given datalake folder, deserialize data and merge data from all files together. It can apply filters on data and respond with filtered data in requested format.

Required external NuGet packages-

1. Microsoft.NET.Sdk.Functions
2. Azure.Storage.Files.DataLake
3. CsvHelper
4. Parquet.Net
5. System.Linq.Dynamic.Core

This HTTP triggered azure function receives request body in below format-

<img width="441" alt="image" src="https://user-images.githubusercontent.com/73296725/213178965-b798973d-aba9-4ee2-a73c-18923c8bf7d7.png">

This function reads from all files from given folder path. Currently, supported formats are **csv, json, parquet**. Files present in folder can be of any of the three formats provided all are matching in terms of schema. In this function, we are using **StudentData** class schema to deserialize data, thus all files should have same schema as per the class.

Each file read from folder will deserialize data into common object. Once the data from all files is collected in common list, we will apply filters given in request, if any.

Finally, we will serialize data as per **outputFileFormat** from request body and respond with file output.
