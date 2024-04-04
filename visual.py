from turso import TursoDBManager  # Replace with your actual file

def generate_html():
    db_manager = TursoDBManager()
    rows = db_manager.get_all_rows_e_bern_raw()

    with open("e_bern_raw_visualization.html", "w", encoding="utf-8") as file:
        file.write("""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>E-Bern Raw Data Visualization</title>
<style>
  .popup, .parsed-pdf-popup { display: none; position: fixed; left: 50%; top: 50%; transform: translate(-50%, -50%); background-color: #f9f9f9; border: 1px solid #ccc; padding: 10px; box-shadow: 0 0 5px rgba(0,0,0,.2); z-index: 1000; max-width: 80%; max-height: 80%; overflow: auto; }
  .close-btn { cursor: pointer; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }
  th { background-color: #f2f2f2; }
  tr:hover {background-color: #f5f5f5;}
  input[type="text"] { width: 100%; padding: 12px 20px; margin-bottom: 12px; }
</style>
<script>
  function togglePopup(id) {
    var popup = document.getElementById("popup-" + id);
    popup.style.display = popup.style.display == "block" ? "none" : "block";
  }
  function toggleParsedPopup(file_name) {
    var popup = document.getElementById("parsed-pdf-popup-" + file_name);
    popup.style.display = popup.style.display == "block" ? "none" : "block";
  }
  function filterTable() {
    var input, filter, table, tr, td, i, txtValue;
    input = document.getElementById("searchInput");
    filter = input.value.toUpperCase();
    table = document.getElementById("data-table");
    tr = table.getElementsByTagName("tr");
    for (i = 0; i < tr.length; i++) {
      td = tr[i].getElementsByTagName("td")[0];
      if (td) {
        txtValue = td.textContent || td.innerText;
        if (txtValue.toUpperCase().indexOf(filter) > -1) {
          tr[i].style.display = "";
        } else {
          tr[i].style.display = "none";
        }
      }       
    }
  }
</script>
</head>
<body>

<input type="text" id="searchInput" onkeyup="filterTable()" placeholder="Search for entries..">

<table id="data-table">
  <tr>
    <th>Forderung</th>
    <th>File Path</th>
    <th>Details</th>
    <th>View Parsed PDF</th> <!-- New column header -->
  </tr>
""")

        for row in rows:
            parsed_row = db_manager.get_row_by_file_name_e_bern_parsed(row[2])  # Assuming row[2] is file_name
            parsed_text = parsed_row[4] if parsed_row else "Parsed text not available."  # New: fetch parsed text

            file.write(f"""
<tr>
  <td>{row[4]}</td>
  <td> <a href='https://entscheidsuche.ch/docs/{row[7]}' > {row[7]} </a> </td>
  <td><button onclick="togglePopup('{row[0]}')">Show Details</button></td>
  <td><button onclick="toggleParsedPopup('{row[2]}')">View Parsed PDF</button></td> <!-- New button -->
</tr>
<div id="popup-{row[0]}" class="popup">
  <span class="close-btn" onclick="togglePopup('{row[0]}')">&times; Close</span><br>
  ID: {row[0]}<br>
  Timestamp: {row[1]}<br>
  File Name: {row[2]}<br>
  Datum: {row[3]}<br>
  Forderung: {row[4]}<br>
  Signatur: {row[5]}<br>
  Source: {row[6]}<br>
  File Path: {row[7]}<br>
  PDF URL: {row[8]}<br>
  Checksum: {row[9]}<br>
  Case Number: {row[10]}<br>
  Scrapy Job: {row[11]}<br>
  Fetch Time UTC: {row[12]}<br>
</div>
<div id="parsed-pdf-popup-{row[2]}" class="parsed-pdf-popup"> <!-- New popup for parsed PDF -->
  <span class="close-btn" onclick="toggleParsedPopup('{row[2]}')">&times; Close</span><br>
  {parsed_text}
</div>
""")

        file.write("""
</table>
</body>
</html>
""")

if __name__ == "__main__":
    generate_html()
    print("HTML file generated successfully.")
