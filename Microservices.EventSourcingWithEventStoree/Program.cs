
#region Bank

using EventStore.Client;
using System.Text.Json;

EventStoreService eventStoreService = new();


AccountCreatedEvent accountCreatedEvent = new()
{
    AccountId = "12345",
    CostumerId = "98765",
    StartBalance = 0,
    Date = DateTime.UtcNow.Date
};
MoneyDepositedEvent moneyDepositedEvent1 = new()
{
    AccountId = "12345",
    Amount = 1000,
    Date = DateTime.UtcNow.Date
};
MoneyDepositedEvent moneyDepositedEvent2 = new()
{
    AccountId = "12345",
    Amount = 500,
    Date = DateTime.UtcNow.Date
};
MoneyWithDrawnEvent moneyWithDrawnEvent = new()
{
    AccountId = "12345",
    Amount = 200,
    Date = DateTime.UtcNow.Date
};
MoneyDepositedEvent moneyDepositedEvent3 = new()
{
    AccountId = "12345",
    Amount = 50,
    Date = DateTime.UtcNow.Date
};
MoneyTransferedEvent moneyTransferedEvent1 = new()
{
    AccountId = "12345",
    Amount = 250,
    Date = DateTime.UtcNow.Date
};
MoneyTransferedEvent moneyTransferedEvent2 = new()
{
    AccountId = "12345",
    Amount = 150,
    Date = DateTime.UtcNow.Date
};
MoneyDepositedEvent moneyDepositedEvent4 = new()
{
    AccountId = "12345",
    Amount = 200,
    Date = DateTime.UtcNow.Date
};

//await eventStoreService.AppendToStreamAsync(
//    streamName: $"customer-{accountCreatedEvent.CostumerId}-stream",
//    new[]
//    {
//        eventStoreService.GenerateEventData(accountCreatedEvent),
//        eventStoreService.GenerateEventData(moneyDepositedEvent1),
//        eventStoreService.GenerateEventData(moneyDepositedEvent2),
//        eventStoreService.GenerateEventData(moneyWithDrawnEvent),
//        eventStoreService.GenerateEventData(moneyDepositedEvent3),
//        eventStoreService.GenerateEventData(moneyTransferedEvent1),
//        eventStoreService.GenerateEventData(moneyTransferedEvent2),
//        eventStoreService.GenerateEventData(moneyDepositedEvent4)
//    }
//    );

BalanceInfo balanceInfo = new();

await eventStoreService.SubscribeToStreamAsync(
    streamName: "customer-98765-stream",
    async (ss, re, ct) =>
    {
        string eventType = re.Event.EventType;
        object @event = JsonSerializer.Deserialize(re.Event.Data.ToArray(), Type.GetType(eventType));

        switch (@event)
        {
            case AccountCreatedEvent e:
                balanceInfo.AccountId = e.AccountId;
                balanceInfo.Balance = e.StartBalance;
                break;
            case MoneyDepositedEvent e:
                balanceInfo.Balance += e.Amount;
                break;
            case MoneyWithDrawnEvent e:
                balanceInfo.Balance -= e.Amount;
                break;
            case MoneyTransferedEvent e:
                balanceInfo.Balance -= e.Amount;
                break;

        }

        await Console.Out.WriteLineAsync("----Balance----");
        await Console.Out.WriteLineAsync(JsonSerializer.Serialize(balanceInfo.Balance));
        await Console.Out.WriteLineAsync("----Balance----");
    }

    );

Console.Read();

class EventStoreService
{
    EventStoreClientSettings GetEventStoreClientSettings(string cString = "esdb://admin:changeit@localhost:2113?tls=false") =>
        EventStoreClientSettings.Create(cString);

    EventStoreClient Client { get => new EventStoreClient(GetEventStoreClientSettings()); }

    public async Task AppendToStreamAsync(string streamName, IEnumerable<EventData> eventData)
        => await Client.AppendToStreamAsync(
            streamName: streamName,
            eventData: eventData,
            expectedState: StreamState.Any
            );

    public EventData GenerateEventData(object @event)
        => new(
            eventId: Uuid.NewUuid(),
            type: @event.GetType().Name,
            data: JsonSerializer.SerializeToUtf8Bytes(@event));

    public async Task SubscribeToStreamAsync(string streamName, Func<StreamSubscription, ResolvedEvent, CancellationToken, Task> eventAppeared)
        => await Client.SubscribeToStreamAsync(
            streamName: streamName,
            start: FromStream.Start,
            eventAppeared: eventAppeared,
            subscriptionDropped: (x, y, z) => Console.WriteLine("disconnected")
            );
}

class BalanceInfo
{
    public string AccountId { get; set; }
    public int Balance { get; set; }
}
public class AccountCreatedEvent
{
    public string AccountId { get; set; }
    public string CostumerId { get; set; }
    public int StartBalance { get; set; }
    public DateTime Date { get; set; }
}
class MoneyDepositedEvent
{
    public string AccountId { get; set; }
    public int Amount { get; set; }
    public DateTime Date { get; set; }
}
class MoneyWithDrawnEvent
{
    public string AccountId { get; set; }
    public int Amount { get; set; }
    public DateTime Date { get; set; }
}
class MoneyTransferedEvent
{
    public string AccountId { get; set; }
    public string TargetACcountId { get; set; }
    public int Amount { get; set; }
    public DateTime Date { get; set; }
}
#endregion





#region INceleme
//using EventStore.Client;
//using System.Text.Json;

//string connectionString = "esdb://admin:changeit@localhost:2113?tls=false";
//var settings = EventStoreClientSettings.Create(connectionString);

//var client = new EventStoreClient(settings);


//OrderPlacedEvent orderPlacedEvent = new OrderPlacedEvent()
//{
//    OrderId = 1,
//    TotalAmount = 1000
//};


//while (true)
//{
//    EventData eventData = new(
//    eventId: Uuid.NewUuid(),
//    type: orderPlacedEvent.GetType().Name,
//    data: JsonSerializer.SerializeToUtf8Bytes(orderPlacedEvent)
//    );

//    await client.AppendToStreamAsync(
//        streamName: "order-stream",
//        expectedState: StreamState.Any,
//        eventData: new[] { eventData }
//        );

//}


//var results = client.ReadStreamAsync(
//    streamName: "order-stream",
//    direction: Direction.Forwards,
//    revision: StreamPosition.Start
//    );

//var datas = await results.ToListAsync();
//Console.WriteLine(datas);
//Console.ReadLine();


// await client.SubscribeToStreamAsync(
//    streamName: "order-stream",
//    start: FromStream.Start,
//    eventAppeared:async (streamSubscription ,resolvedEvent,cancellationToken) =>
//    {
//        OrderPlacedEvent @event= JsonSerializer.Deserialize<OrderPlacedEvent>(resolvedEvent.Event.Data.ToArray());
//        await Console.Out.WriteLineAsync(JsonSerializer.Serialize(@event));
//    },
//    subscriptionDropped:(streamSubscription ,subscriptionDroppedreason, exception) =>Console.WriteLine("Disconnected")

//    );

//Console.Read();



//class OrderPlacedEvent
//{
//    public int OrderId { get; set; }
//    public int TotalAmount { get; set; }
//}
#endregion



