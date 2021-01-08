namespace AdfOutputReader.KeywordObject {
    public class EventKeyword {
        private string eventId;
        public string EventId
        {   get {return eventId;}
            set { eventId = value;}
        }

        private string values;
        public string Values
        {   get {return values;}
            set { values = value;}
        }

        public EventKeyword (string eventId, string values) {
            this.eventId = eventId;
            this.values = values;
        }
    }
}
