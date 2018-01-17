using Orleans;
using Orleans.Streams;
using StreamDTO;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using RB.OrleansClient;

namespace OutBoundClient
{
    class Program
    {
        static void Main(string[] args)
        {
            var runner = Task.Run(async () =>
            {
                //see unit tests instead
                var config = new ClientConfiguration();

                config.GatewayProvider = ClientConfiguration.GatewayProviderType.SqlServer;
                config.DataConnectionString =
                    @"Server=NCI-R5ESQL01.dev-r5ead.net\MSSQLSVR02;Database=orleans;User ID=orleans;password=orleans;";
                config.DeploymentId = "R5Ent-v1.0";

                config.AddSimpleMessageStreamProvider("NCI-BRC");
                config.AddSimpleMessageStreamProvider("NCI-PCC");

                config.DefaultTraceLevel = Severity.Warning;


                IClientGrainFactory client = new ClientGrainFactory(config);
                //var brcStreamProv = await client.GetStreamProviderAsync("NCI-BRC");
                //var pccStreamProv = await client.GetStreamProviderAsync("NCI-PCC");



                if (args.Count() > 1)
                {
                    if (args.Count() != 3)
                    {
                        Console.WriteLine(
                            @"wrong number of argument.... should be like this: .\OutBoundClient.exe b01efcfa-ee7b-4e72-9f06-edacebd79f8f NCI-PCC 1*902");
                        Environment.Exit(1);
                    }
                    var streamNamespace = args[2];
                    Guid facilityId;
                    if (!Guid.TryParse(args[0], out facilityId))
                    {
                        Console.WriteLine(
                            @"first arg should be a Guid, like this: .\OutBoundClient.exe b01efcfa-ee7b-4e72-9f06-edacebd79f8f NCI-PCC 1*902");
                        Environment.Exit(2);
                    }

                    var streamProv = await client.GetStreamProviderAsync(args[1]);

                    var stream = streamProv.GetStream<ExpandoObject>(facilityId, streamNamespace);

                    var streamHandle = await stream.SubscribeAsync((o, token) =>
                        {
                            Console.WriteLine(o.ToString());

                            return TaskDone.Done;
                        }, exception =>
                        {
                            Console.WriteLine(exception);
                            return TaskDone.Done;
                        },
                        () =>
                        {
                            return TaskDone.Done;
                        });

                }
                else
                {
                    var factory = await client.GetGrainFactoryAsync();
                    var management = factory.GetGrain<IManagementGrain>(0);

                    var grainStats = await management.GetSimpleGrainStatistics();

                    var details =
                        await
                            management.GetDetailedGrainStatistics(new string[] {"Orleans.Streams.PubSubRendezvousGrain"});

                    //Console.WriteLine(grainStats.ToString());
                    //Console.WriteLine(details.ToString());


                    Console.WriteLine("Current List of PubSubRendezvousGrains");
                    Console.WriteLine(
                        "Run this executable with one of these lines as the argument to view a live stream");

                    foreach (var detailedGrainStatistic in details)
                    {
                        string keyext;
                        Guid facilityId = detailedGrainStatistic.GrainIdentity.GetPrimaryKey(out keyext);
                        var streamNamespace = keyext.Split('_');
                        Console.WriteLine($"{facilityId} {streamNamespace[0]} {streamNamespace[1]}");
                    }
                }

            });


            runner.Wait();


          Console.ReadLine();

        }
    }
}