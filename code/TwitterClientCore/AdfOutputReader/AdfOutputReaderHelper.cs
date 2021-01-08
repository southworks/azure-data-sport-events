using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using CsvHelper;
using AdfOutputReader.models;
using AdfOutputReader.Mappers;
using AdfOutputReader.KeywordObject;

namespace AdfOutputReader
{
    public class AdfOutputReaderHelper
    {
        string keywordPath;
        string synonymsPath;

        public AdfOutputReaderHelper(string keywordPath, string synonymsPath) {
            this.keywordPath = keywordPath;
            this.synonymsPath = synonymsPath;
        }
    
        public List<EventKeyword> ReadAdfOutputFile(List<TeamSynonym> synonymsList) {  
            List<EventKeyword> result = new List<EventKeyword>();
            try {  
                using(var reader = new StreamReader(keywordPath, Encoding.Default))  
                using(var csv = new CsvReader(reader, System.Globalization.CultureInfo.CurrentCulture)) {  
                    csv.Configuration.RegisterClassMap<AdfOutputMap>();
                    csv.Configuration.HeaderValidated = null; 
                    csv.Configuration.MissingFieldFound = null;
                    var records = csv.GetRecords<AdfOutput>().ToList();

                    foreach (AdfOutput elem in records) {
                        string keywordsMerged = MergeSynonymsListToKeywordsList(synonymsList, elem.Keywords);
                        EventKeyword currentKeyword = CreateKeywordElement(elem.IdEvent, keywordsMerged);
                        result.Add(currentKeyword);
                    }

                    return result;  
                }  
            } catch (Exception e) {  
                throw new Exception(e.Message);  
            }  
        }

        public List<TeamSynonym> ReadSynonymsFile() {
            List<TeamSynonym> result = new List<TeamSynonym>();
            try {  
                using(var reader = new StreamReader(synonymsPath, Encoding.Default))
                {
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        string[] values = line.Split(',');
                        string teamName = values[0];
                        List<string> synonyms = new List<string>();

                        for (int index=1; index < values.Length; index++) {
                            string currentValue = values[index];
                            synonyms.Add(currentValue);
                        }

                        TeamSynonym synonym = new TeamSynonym(teamName, synonyms);
                        result.Add(synonym);
                    }

                    return result;
                }
            } catch (Exception e) {  
                throw new Exception(e.Message);  
            }  
        }

        public EventKeyword CreateKeywordElement(string idEvent, string keywordValues) {
            string keywordsParsed = keywordValues.Replace(",,", ",");
            EventKeyword newKeyword = new EventKeyword(idEvent,keywordsParsed);
            
            return newKeyword;
        }

        public string MergeSynonymsListToKeywordsList(List<TeamSynonym> synonymsList, string keywordList) {
            string result = String.Empty;
            foreach (TeamSynonym s in synonymsList) {
                if (s.Synonyms.Count > 0) {
                    string teamToFind = s.TeamName;
                    string[] keywordValuesSplited = keywordList.Split(",");
                    foreach (string st in keywordValuesSplited) {
                        if (st.Contains(teamToFind)) {
                            foreach (string synonym in s.Synonyms) {
                                string newValue = st.Replace(teamToFind, synonym);
                                result += newValue + ",";
                            }
                        }
                    }
                }
            }
            result += keywordList;
            return result;
        }
    }
}