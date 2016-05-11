using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamDTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamTestGrains
{
    public interface ISignatureBaseEaterGrain: IGrainWithGuidKey
    {
        Task<int> GetCountOfReceivedStreamMessages();
    }


    [ImplicitStreamSubscription(@"BRC0-in")]
    [ImplicitStreamSubscription(@"BRC1-in")]
    [ImplicitStreamSubscription(@"BRC2-in")]
    [ImplicitStreamSubscription(@"BRC3-in")]
    [ImplicitStreamSubscription(@"BRC4-in")]
    [ImplicitStreamSubscription(@"BRC5-in")]
    [ImplicitStreamSubscription(@"BRC6-in")]
    [ImplicitStreamSubscription(@"BRC7-in")]
    [ImplicitStreamSubscription(@"BRC8-in")]
    public class BaseEaterGrain : Grain, ISignatureBaseEaterGrain
    {
        private Logger logger;
        private IAsyncStream<SignatureBase> stream;
        private int counter;

        public override async Task OnActivateAsync()
        {
            logger = base.GetLogger("BaseEaterGrain " + base.IdentityString);
            logger.Info("OnActivateAsync");

            IStreamProvider streamProvider = base.GetStreamProvider(Utils.Constants.StreamProvider);
            counter = 0;

            //needs some sort of loop for every namespace
            object[] attrs = GetType().GetCustomAttributes(typeof(ImplicitStreamSubscriptionAttribute), true);
            foreach (ImplicitStreamSubscriptionAttribute implictSub in attrs)
            {
                var streamNamespace = implictSub.Namespace;

                stream = streamProvider.GetStream<SignatureBase>(this.GetPrimaryKey(), streamNamespace);


                await stream.SubscribeAsync(
        (e, t) =>
        {
            logger.Info("Received a event {0}", e);
            counter++;
            return TaskDone.Done;
        },
        (e) =>
        {
            logger.Warn(0, "failed", e);
            return TaskDone.Done;
        },
        () =>
        {
            logger.Warn(0, "completed");
            return TaskDone.Done;
        }
        );

            }
        }



        public Task<int> GetCountOfReceivedStreamMessages()
        {
            return Task.FromResult(counter);
        }
    }





}
