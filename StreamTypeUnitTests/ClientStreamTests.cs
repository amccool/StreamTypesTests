using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.TestingHost;
using System.Diagnostics;

using Orleans.Runtime.Configuration;
using System.Threading.Tasks;
using Orleans;
using Orleans.TestingHost.Utils;
using Orleans.Streams;
using StreamDTO;
using System.Collections.Generic;
using System.Linq;

namespace StreamTypeUnitTests
{
    [TestClass]
    public class ClientStreamTests
    {
        private static TestCluster testingCluster;

        private readonly TimeSpan _timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(10);

        private TestContext testContextInstance;

        public TestContext TestContext
        {
            get { return testContextInstance; }
            set { testContextInstance = value; }
        }

        public ClientStreamTests()
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
            var options = new TestClusterOptions();

            options.ClusterConfiguration.AddMemoryStorageProvider("PubSubStore");
            options.ClusterConfiguration.AddSimpleMessageStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);
            options.ClusterConfiguration.Globals.ClientDropTimeout = TimeSpan.FromSeconds(5);
            options.ClusterConfiguration.Defaults.DefaultTraceLevel = Orleans.Runtime.Severity.Warning;

            options.ClientConfiguration.AddSimpleMessageStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);
            options.ClientConfiguration.DefaultTraceLevel = Orleans.Runtime.Severity.Warning;

            return new TestCluster(options);
        }



        [TestMethod]
        public async Task SubClassFromStreamBaseClassTest()
        {

            Guid streamId = Guid.NewGuid();

        IStreamProvider streamProvider = Orleans.GrainClient.GetStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);
            IAsyncStream<SignatureBase> messageStream =
                streamProvider.GetStream<SignatureBase>(streamId, StreamTestGrains.Utils.Constants.StreamNamespace);

            int wantedAmount = 1000;


            int received = 0;

            StreamSubscriptionHandle<SignatureBase> subscriptionHandle = await messageStream.SubscribeAsync<SignatureBase>
                (
                    (m, s) =>
                    {
                        received++;

                        Assert.IsInstanceOfType(m, typeof(SecondSimple));
                        Console.WriteLine(m.GetType().ToString());
                        return TaskDone.Done;
                    },
                    (e) =>
                    {
                        Console.WriteLine(e);
                        return TaskDone.Done;
                    },
                    () =>
                    {
                        Console.WriteLine("we completred");
                        return TaskDone.Done;
                    }
                );

            await LoadStream(messageStream, wantedAmount);

            Assert.AreEqual<int>(wantedAmount, received);
        }


        async Task LoadStream(IAsyncStream<SignatureBase> messageStream, int count)
        {
            for (int i = 0; i < count; i++)
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


        [TestMethod]
        public async Task MultipleSubClassFromStreamBaseClassTest()
        {

            Guid streamId = Guid.NewGuid();

            IStreamProvider streamProvider = Orleans.GrainClient.GetStreamProvider(StreamTestGrains.Utils.Constants.StreamProvider);
            IAsyncStream<SignatureBase> messageStream =
                streamProvider.GetStream<SignatureBase>(streamId, StreamTestGrains.Utils.Constants.StreamNamespace);

            int wantedFirstSimpleAmount = 1000;
            int wantedSecondSimpleAmount = 1000;

            int received = 0;

            StreamSubscriptionHandle<SignatureBase> subscriptionHandle = await messageStream.SubscribeAsync<SignatureBase>
                (
                    (m, s) =>
                    {
                        Assert.IsInstanceOfType(m, typeof(SignatureBase));

                        if (received < wantedFirstSimpleAmount)
                        {
                            Assert.IsInstanceOfType(m, typeof(FirstSimple));
                        }
                        else
                        {
                            Assert.IsInstanceOfType(m, typeof(SecondSimple));
                        }
                        received++;
                        Console.WriteLine(m.GetType().ToString());
                        return TaskDone.Done;
                    },
                    (e) =>
                    {
                        Console.WriteLine(e);
                        return TaskDone.Done;
                    },
                    () =>
                    {
                        Console.WriteLine("we completred");
                        return TaskDone.Done;
                    }
                );

            await LoadStream(messageStream, wantedFirstSimpleAmount, wantedSecondSimpleAmount);

            Assert.AreEqual<int>(wantedFirstSimpleAmount + wantedSecondSimpleAmount, received);
        }

        async Task LoadStream(IAsyncStream<SignatureBase> messageStream, int countFirstSimple, int countSecondSimple)
        {
            //we want synchronous

            for (int i = 0; i < countFirstSimple; i++)
            {
                await messageStream.OnNextAsync(new FirstSimple()
                {
                    AnotherBigNumber = 10000000,
                    Identifer = Guid.NewGuid(),
                    MyName = "bill",
                    UserName = "tom"
                });
            }


            for (int i = 0; i < countSecondSimple; i++)
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
    }
}
