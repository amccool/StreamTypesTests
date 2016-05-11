using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Diagnostics;
using Orleans.TestingHost;
using Orleans.Runtime.Configuration;
using System.Threading.Tasks;
using Orleans.TestingHost.Utils;
using Orleans.Runtime;
using Orleans;
using StreamTestGrains;
using Orleans.Streams;
using StreamDTO;
using System.Linq;

namespace StreamTypeUnitTests
{
    [TestClass]
    public class ImplicitGrainStreamTests
    {
        private static TestCluster testingCluster;

        public Logger logger
        {
            get { return GrainClient.Logger; }
        }

        private static readonly Guid _StreamId = Guid.NewGuid();

        private readonly TimeSpan _timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(10);

        private TestContext testContextInstance;

        public TestContext TestContext
        {
            get { return testContextInstance; }
            set { testContextInstance = value; }
        }

        public ImplicitGrainStreamTests()
        {
        }


        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            testingCluster = CreateTestCluster();
            testingCluster.Deploy();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            // Optional. 
            // By default, the next test class which uses TestignSiloHost will
            // cause a fresh Orleans silo environment to be created.
            testingCluster.StopAllSilos();
        }


        public static TestCluster CreateTestCluster()
        {
            var options = new TestClusterOptions(10);

            options.ClusterConfiguration.AddMemoryStorageProvider("PubSubStore");
            options.ClusterConfiguration.AddSimpleMessageStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);
            options.ClusterConfiguration.Globals.ClientDropTimeout = TimeSpan.FromSeconds(5);
            options.ClusterConfiguration.Defaults.DefaultTraceLevel = Orleans.Runtime.Severity.Warning;

            options.ClientConfiguration.AddSimpleMessageStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);
            options.ClientConfiguration.DefaultTraceLevel = Orleans.Runtime.Severity.Warning;

            return new TestCluster(options);
        }

        [TestMethod]
        public async Task EarlyReferenceConsumeAnImplicitStreamTest()
        {
            Guid streamId = Guid.NewGuid();

            IStreamProvider streamProvider = Orleans.GrainClient.GetStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);
            IAsyncStream<SignatureBase> messageStream =
                streamProvider.GetStream<SignatureBase>(streamId, @"BRC0-in");

            int wantedAmount = 1000;
            int received = 0;

            //// consumer joins first, producer later
            var consumer = GrainClient.GrainFactory.GetGrain<ISignatureBaseEaterGrain>(streamId);

            for (int i = 0; i < wantedAmount; i++)
            {
                await messageStream.OnNextAsync(new SecondSimple()
                {
                    BigNumber = 10,
                    Identifer = Guid.NewGuid(),
                    MyName = "bill",
                    UserName = "tom"
                });
            }


            received = await consumer.GetCountOfReceivedStreamMessages();

            Assert.AreEqual<int>(wantedAmount, received);
        }


        [TestMethod]
        public async Task LateReferenceConsumeAnImplicitStreamTest()
        {
            Guid streamId = Guid.NewGuid();

            IStreamProvider streamProvider = Orleans.GrainClient.GetStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);
            IAsyncStream<SignatureBase> messageStream =
                streamProvider.GetStream<SignatureBase>(streamId, @"BRC0-in");

            int wantedAmount = 1000;
            int received = 0;


            for (int i = 0; i < wantedAmount; i++)
            {
                await messageStream.OnNextAsync(new SecondSimple()
                {
                    BigNumber = 10,
                    Identifer = Guid.NewGuid(),
                    MyName = "bill",
                    UserName = "tom"
                });
            }


            var consumer = GrainClient.GrainFactory.GetGrain<ISignatureBaseEaterGrain>(streamId);
            received = await consumer.GetCountOfReceivedStreamMessages();

            Assert.AreEqual<int>(wantedAmount, received);
        }



        [TestMethod]
        public async Task MultipleNamespacesImplicitStreamTest()
        {
            var listOfNameSpaces = new string[] {
        @"BRC0-in",
        @"BRC1-in",
        @"BRC2-in",
        @"BRC3-in",
        @"BRC4-in",
        @"BRC5-in",
        @"BRC6-in",
        @"BRC7-in",
        @"BRC8-in",
    };


            Guid streamId = Guid.NewGuid();

            IStreamProvider streamProvider = Orleans.GrainClient.GetStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);

                int wantedAmount = 100;
                int received = 0;
            foreach (var streamNamespace in listOfNameSpaces)
            {
                IAsyncStream<SignatureBase> messageStream =
                    streamProvider.GetStream<SignatureBase>(streamId, streamNamespace);



                for (int i = 0; i < wantedAmount; i++)
                {
                    await messageStream.OnNextAsync(new SecondSimple()
                    {
                        BigNumber = 10,
                        Identifer = Guid.NewGuid(),
                        MyName = "bill",
                        UserName = "tom"
                    });
                }
            }

            var consumer = GrainClient.GrainFactory.GetGrain<ISignatureBaseEaterGrain>(streamId);
            received = await consumer.GetCountOfReceivedStreamMessages();

            Assert.AreEqual<int>(wantedAmount * listOfNameSpaces.Count(), received);
        }
    }
}
