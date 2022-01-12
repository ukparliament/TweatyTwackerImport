namespace TweatyTwackerImport
{
    using System;
    using System.Configuration;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using VDS.RDF;
    using VDS.RDF.Query;
    using VDS.RDF.Storage;

    class Program
    {
        static void Main(string[] args)
        {
            string sparqlEndpoint = ConfigurationManager.AppSettings["TweatyTwackerSparqlEndpoint"];
            int dayOffset = int.Parse(ConfigurationManager.AppSettings["TweatyTwackerDayOffset"]);
            var from_date = DateTime.Today.AddDays(dayOffset).ToString("yyyy-MM-dd");
            var to_date = DateTime.Today.ToString("yyyy-MM-dd");

            string query = $@"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                  PREFIX : <https://id.parliament.uk/schema/>
                  select ?Treaty ?Treatyname ?LeadOrg ?Series ?Link ?workPackage ?procStepName ?itemDate where {{
                    ?Treaty a :Treaty .  
                    ?Treaty rdfs:label ?Treatyname .
                    OPTIONAL {{ ?Treaty :treatyHasLeadGovernmentOrganisation/ rdfs:label ?LeadOrg .}} 
                    OPTIONAL {{ ?Treaty :treatyHasSeriesMembership/ :seriesItemCitation ?Series.}}
                    OPTIONAL {{ ?Treaty :workPackagedThingHasWorkPackagedThingWebLink ?Link.}}
                    ?Treaty :workPackagedThingHasWorkPackage ?workPackage.
                    ?workPackage :workPackageHasProcedure/rdfs:label ?proc.
                    FILTER(?proc IN (""Treaties subject to the Constitutional Reform and Governance Act 2010""))
                      ?workPackage :workPackageHasBusinessItem/:businessItemHasProcedureStep ?procStep;
                        :workPackageHasBusinessItem ?busItem .
                      ?busItem :businessItemHasProcedureStep/rdfs:label ?itemDate2;
                        :businessItemDate ?itemDate.
                      ?procStep rdfs:label ?procStepName.
                    FILTER(?procStepName IN (""Laid before the House of Commons""))
                    FILTER(?itemDate2 IN (""Laid before the House of Commons""))
                    FILTER(str(?itemDate) >= '{from_date}' && str(?itemDate) <= '{to_date}')}}
                    ";
            using (var connector = new SparqlConnector(new Uri(sparqlEndpoint)))
            {
                var results = connector.Query(query) as SparqlResultSet;
                if (results.Any())
                {
                    string connectionString = ConfigurationManager.ConnectionStrings["TweatyTwackerSqlServer"].ConnectionString;
                    Console.WriteLine($"Sql: {connectionString}");
                    SqlConnection connection = new SqlConnection(connectionString);
                    connection.Open();

                    foreach (var result in results)
                    {
                        INode node;
                        Treaty treaty = new Treaty();
                        result.TryGetValue("Treaty", out node);
                        treaty.Id = (node as UriNode).Uri.ToString();
                        result.TryGetValue("Treatyname", out node);
                        treaty.Name = (node as LiteralNode).Value.Trim();
                        result.TryGetValue("LeadOrg", out node);
                        treaty.LeadOrganisation = (node as LiteralNode).Value.Trim();
                        result.TryGetValue("Series", out node);
                        treaty.Series = (node as LiteralNode).Value.Trim();
                        result.TryGetValue("workPackage", out node);
                        treaty.WorkPackageId = (node as UriNode).Uri.ToString().Trim();
                        result.TryGetValue("itemDate", out node);
                        treaty.LaidDate = DateTime.Parse((node as LiteralNode).Value.Trim());
                        result.TryGetValue("Link", out node);
                        treaty.Link = (node as UriNode).Uri.ToString().Trim();

                        using (SqlCommand cmd = new SqlCommand("Add to database", connection))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.CommandText = "InsertUpdateTweatyTwackerTreaty";
                            cmd.Parameters.AddWithValue("@TreatyName", treaty.Name != null ? (object)treaty.Name : DBNull.Value);
                            cmd.Parameters.AddWithValue("@LeadOrg", treaty.Name != null ? (object)treaty.LeadOrganisation : DBNull.Value);
                            cmd.Parameters.AddWithValue("@Series", treaty.Name != null ? (object)treaty.Series : DBNull.Value);
                            cmd.Parameters.AddWithValue("@LaidDate", treaty.LaidDate != null ? (object)treaty.LaidDate : DBNull.Value);
                            cmd.Parameters.AddWithValue("@TreatyUri", treaty.Id != null ? (object)treaty.Id : DBNull.Value);
                            cmd.Parameters.AddWithValue("@WorkPackageUri", treaty.WorkPackageId != null ? (object)treaty.WorkPackageId : DBNull.Value);
                            cmd.Parameters.AddWithValue("@TnaUri", treaty.Link != null ? (object)treaty.Link : DBNull.Value);
                            cmd.Parameters.AddWithValue("@IsTweeted", (object)0);
                            cmd.Parameters.Add("@Message", SqlDbType.NVarChar, 50).Direction = ParameterDirection.Output;
                            cmd.ExecuteNonQuery();
                            string msg = cmd.Parameters["@Message"].Value.ToString();
                            Console.WriteLine($"Title: {treaty.Id}, {msg}");
                        }
                    }
                    connection.Close();
                }
            }

        }
    }
}
