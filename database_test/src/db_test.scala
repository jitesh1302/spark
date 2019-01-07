
  
  //package tests

import java.sql.{Connection,DriverManager}

object db_test extends App {
    // connect to the database named "mysql" on port 8889 of localhost
    val url ="jdbc:mysql://localhost:3306/demo?user=demo&password=demo"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "demo"
    val password = "demo"
    var connection:Connection = _
    try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement
        val rs = statement.executeQuery("SELECT * FROM Master_table")
        while (rs.next) {
            val Subject_Area = rs.getString("Subject_Area")
            val Source_Type = rs.getString("Source_Type")
             val Username = rs.getString("Username")
              val Password = rs.getString("Password")
               val Servername = rs.getString("Servername")
                val Table_FileMask = rs.getString("Table_FileMask")
                 val Extract_Type = rs.getString("Extract_Type")
                  val Data_Type = rs.getString("Data_Type")
                   val Last_Extract = rs.getString("Last_Extract")
                   
           println(Subject_Area,Source_Type,Username,Password, Servername,Table_FileMask,Extract_Type,Data_Type,Last_Extract)
            //println("id = %s, name = %s".format(Subject_Area,Source_Type,Username,Password, Servername,Table_FileMask,Extract_Type,Data_Type,Last_Extract))
        
    } 
        
    }catch {
        case e: Exception => e.printStackTrace
    }
    connection.close
}
  
//"jdbc:mysql://localhost:3306/mysql/?user=root &password=ubuntu"
  
  