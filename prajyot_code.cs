using System;
using System.Data;
using Microsoft.Data.SqlClient;

class Program
{
    static void Main()
    {
        string connectionString = @"Server=****************** ;Database=***************         ;Trusted_Connection=True;Encrypt=False;";
        //  please enter database and credentials as per your system/database etc.
  
         string queryTemplate = "SELECT * FROM mocK_data ORDER BY (SELECT NULL)  OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY";
        
        int pageSize = 5;  // Number of rows per chunk
        int pageNumber = 0;  // Start from the first page
        
        // Create a new DataTable to store all the data
        DataTable prajyotDataTable = new DataTable();

        // Loop through the data in chunks
        DataSet resultData;
        do
        {
            // Construct the query for the current page
            string query = string.Format(queryTemplate, pageNumber * pageSize, pageSize);

            // Fetch data from the database using the FetchData method
            resultData = FetchData(connectionString, query);

            // Check if the DataSet has any data
            if (resultData.Tables.Count > 0 && resultData.Tables[0].Rows.Count > 0)
            {
                // Set the columns of the DataTable only once, based on the first chunk
                if (prajyotDataTable.Columns.Count == 0)
                {
                    prajyotDataTable = resultData.Tables[0].Copy();  // Copy structure of the first result
                }
                else
                {
                    // Add rows to the DataTable
                    foreach (DataRow row in resultData.Tables[0].Rows)
                    {
                        prajyotDataTable.ImportRow(row);  // Import rows one by one
                    }
                }

                pageNumber++;  // Move to the next page
            }
            else
            {
                Console.WriteLine("No more data to fetch.");
            }

        } while (resultData.Tables.Count > 0 && resultData.Tables[0].Rows.Count > 0);  // Continue fetching until there is no more data

        // Print the data from the DataTable at the end
        if (prajyotDataTable.Rows.Count > 0)
        {
            Console.WriteLine("Data from the DataTable:");
            foreach (DataRow row in prajyotDataTable.Rows)
            {
                foreach (DataColumn column in prajyotDataTable.Columns)
                {
                    Console.Write($"{column.ColumnName}: {row[column]}   ");
                }
                Console.WriteLine();
            }
        }
        else
        {
            Console.WriteLine("No data in the DataTable.");
        }
    }

    // Method to fetch data from the database and return a DataSet
    static DataSet FetchData(string connectionString, string query)
    {
        // Create a DataSet to store the fetched data
        DataSet dataSet = new DataSet();

        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            try
            {
                // Open the connection
                connection.Open();
                Console.WriteLine("Connection successful.");

                // Create a SqlDataAdapter to fetch data
                using (SqlDataAdapter adapter = new SqlDataAdapter(query, connection))
                {
                    // Fill the DataSet with the query result
                    adapter.Fill(dataSet);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }

        return dataSet;
    }
}
