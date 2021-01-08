using AdfOutputReader.models;
using CsvHelper.Configuration;

namespace AdfOutputReader.Mappers {  
    public sealed class AdfOutputMap: ClassMap<AdfOutput> {  
        public AdfOutputMap() {  
            Map(x => x.IdEvent).Name("idEvent");
            Map(x => x.StrEvent).Name("strEvent");
            Map(x => x.DateEvent).Name("dateEvent");
            Map(x => x.StrStadium).Name("strStadium");
            Map(x => x.StrThumb).Name("strThumb");
            Map(x => x.StrReferee).Name("strReferee");
            Map(x => x.IdHomeTeam).Name("idHomeTeam");
            Map(x => x.HomeTeamName).Name("homeTeamName");
            Map(x => x.HomeTeamBadge).Name("homeTeamBadge");
            Map(x => x.IdAwayTeam).Name("idAwayTeam");
            Map(x => x.AwayTeamName).Name("awayTeamName");
            Map(x => x.AwayTeamBadge).Name("awayTeamBadge");
            Map(x => x.Keywords).Name("keywords");
        }  
    }  
}