using System.Collections.Generic;

namespace AdfOutputReader.models
{
    public class TeamSynonym 
    {
        public TeamSynonym (string TeamName, List<string> Synonyms) {
            this.TeamName = TeamName;
            this.Synonyms = Synonyms;
        }
        public string TeamName {get; set;}
        public List<string> Synonyms;
    }
}