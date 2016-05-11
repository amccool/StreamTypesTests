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

namespace StreamTypeUnitTests
{
    [TestClass]
    public class GrainStreamTests
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

        public GrainStreamTests()
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
            var options = new TestClusterOptions(3);

            options.ClusterConfiguration.AddMemoryStorageProvider("PubSubStore");
            options.ClusterConfiguration.AddSimpleMessageStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);
            options.ClusterConfiguration.Globals.ClientDropTimeout = TimeSpan.FromSeconds(5);
            options.ClusterConfiguration.Defaults.DefaultTraceLevel = Orleans.Runtime.Severity.Warning;

            options.ClientConfiguration.AddSimpleMessageStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);
            options.ClientConfiguration.DefaultTraceLevel = Orleans.Runtime.Severity.Warning;

            return new TestCluster(options);
        }

        [TestMethod]
        public async Task StreamingTests_Consumer_Producer()
        {
            Guid streamId = Guid.NewGuid();



            // consumer joins first, producer later
            var consumer = GrainClient.GrainFactory.GetGrain<ISampleStreaming_ConsumerGrain>(Guid.NewGuid());
            await consumer.BecomeConsumer(streamId, StreamTestGrains.Utils.Constants.StreamNamespace, StreamTestGrains.Utils.Constants.StreamProvider);

            var producer = GrainClient.GrainFactory.GetGrain<ISampleStreaming_ProducerGrain>(Guid.NewGuid());
            await producer.BecomeProducer(streamId, StreamTestGrains.Utils.Constants.StreamNamespace, StreamTestGrains.Utils.Constants.StreamProvider);

            await producer.StartPeriodicProducing();

            await Task.Delay(TimeSpan.FromMilliseconds(1000));

            await producer.StopPeriodicProducing();

            await TestingUtils.WaitUntilAsync(lastTry => CheckCounters(producer, consumer, lastTry), _timeout);

            await consumer.StopConsuming();
        }

        private async Task<bool> CheckCounters(ISampleStreaming_ProducerGrain producer, ISampleStreaming_ConsumerGrain consumer, bool assertIsTrue)
        {
            var numProduced = await producer.GetNumberProduced();
            var numConsumed = await consumer.GetNumberConsumed();
            logger.Info("CheckCounters: numProduced = {0}, numConsumed = {1}", numProduced, numConsumed);
            if (assertIsTrue)
            {
                Assert.AreEqual(numProduced, numConsumed, String.Format("numProduced = {0}, numConsumed = {1}", numProduced, numConsumed));
                return true;
            }
            else
            {
                return numProduced == numConsumed;
            }
        }

    }
}
