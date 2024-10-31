using MongoDB.Bson;
using MongoDB.Driver;

//de onde
string sourceConnectionString = "mongodb://guest:guest@localhost:27018";
//para onde
string destinationConnectionString = "mongodb://guest:guest@localhost:27017";
// Nomes dos bancos de dados e coleções
string sourceDatabaseName = "";
bool dropCollection = false;
string justCollectionName = "";
int batchSize = 1000;

/****************EXECUTE**************************************/

var liveNames = new List<string>() { "live", "lv", "production" };

if (liveNames.Contains(destinationConnectionString.ToLower()))
{
    Console.WriteLine($"ERRO: ATENÇÃO: DATABASE SOURCE ESTA APONTANDO PARA LIVE/PRODUCTION, NÃO É PERMITIDO RESTAURAR LIVE");
    return;
}

var sourceClient = new MongoClient(sourceConnectionString);
var destinationClient = new MongoClient(destinationConnectionString);

var sourceDatabase = sourceClient.GetDatabase(sourceDatabaseName);
var destinationDatabase = destinationClient.GetDatabase(sourceDatabaseName);

if (dropCollection)
    await destinationClient.DropDatabaseAsync(sourceDatabaseName);

Console.WriteLine($"############ INICIANDO PROCESSO {DateTime.Now}  ########################################");

using (var cursor = await sourceDatabase.ListCollectionNamesAsync())
{
    var collections = await cursor.ToListAsync();
    List<Task> copyTasks = new();

    foreach (var collectionName in collections)
    {
        // Verifica se deve copiar apenas uma coleção específica
        if (!string.IsNullOrEmpty(justCollectionName) && justCollectionName.ToLower() != collectionName.ToLower())
        {
            continue;
        }

        // Adiciona uma nova tarefa de cópia para cada coleção
        copyTasks.Add(Task.Run(async () =>
        {
            var sourceCollection = sourceDatabase.GetCollection<BsonDocument>(collectionName);
            var destinationCollection = destinationDatabase.GetCollection<BsonDocument>(collectionName);
            if (dropCollection)
                await destinationCollection.DeleteManyAsync(new BsonDocument());

            var filter = new BsonDocument();
            var options = new FindOptions<BsonDocument>
            {
                BatchSize = batchSize,
                // Sort = Builders<BsonDocument>.Sort.Descending("GenerateDate")  //setar propriedade
            };

            // Processamento em lotes usando um cursor
            using (var sourceCursor = await sourceCollection.FindAsync(filter, options))
            {
                // Para cada lote de documentos retornado pelo cursor
                while (await sourceCursor.MoveNextAsync())
                {
                    var batch = sourceCursor.Current.ToList();

                    if (batch.Count > 0)
                    {
                        Console.WriteLine($"Inserindo {batch.Count} documentos em '{collectionName}'...");
                        await destinationCollection.InsertManyAsync(batch); // Inserimos o lote na coleção de destino
                    }
                }
            }

            Console.WriteLine($"Coleção '{collectionName}' : SUCESSO!");
        }));
    }

    await Task.WhenAll(copyTasks);
    Console.WriteLine($"############ FINALIZADO {DateTime.Now} ########################################");
}