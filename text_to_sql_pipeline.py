import mysql.connector
import datetime
import unicodedata
import tkinter as tk
from tkinter import filedialog
from tkinter import ttk
import pandas
import matplotlib.pyplot as plt


# Global Variables

sql_host = ""

sql_name = ""

sql_password = ""

user_file = ""

message_data = []

word_data = []

stop_words = ["are", "was", "the", "and", "to", "is", "of", "a"]

lines = []

database_name = ""

data_frames = []

all_tables = ["all_messages", "all_words", "word_stats", "member_stats", "date_stats"]



# ------------------------------------- TEXT FILE PARSING, EXTRACTING, CALCULATIONS ---------------------------------------



def setup():


    # Extracting the name of the text file without the file type, and creating an SQL database using that name

    file_name = (user_file.split("/")[-1].split(".", 1)[0])

    global database_name
    database_name = f"{file_name}_chat_data"



    # Reading the given file and storing indiviual lines, messages (just the name and message, without the data/time), the unique names/members that appear in the chat, and all the datetimes and unique datetimes that appear

    file = open(user_file, "r", encoding="utf-8", errors='ignore')

    content = file.read()

    global lines
    lines = content.splitlines()[3:]


    # Determining if the text file is an export from an apple device or not (text formatting is different for each)

    apple = False

    if content[0] == "[":
        apple = True


    return apple



def clean_unicode(s):
    s = unicodedata.normalize("NFKC", s)
    return "".join(c for c in s if not unicodedata.category(c).startswith("C"))



def android_parsing():

    for line in lines:

        if (" - " not in line):
            continue

        if ("changed the group name" in line):
            continue

        timestamp_part, message_part = line.split(" - ", 1)

        if (":" not in message_part):
            continue


        timestamp_part = clean_unicode(timestamp_part)

        dt = datetime.datetime.strptime(timestamp_part, "%m/%d/%y, %I:%M %p")

        y = dt.year
        m = dt.month
        d = dt.day
        h = dt.hour
        min = dt.minute


        if len(message_part.split(":")) >= 2:
            n, msg = message_part.split(":", 1)
            f, l = n.split(" ", 1)

            msg = msg.replace("\u200e", "").replace("\u202f", "").replace("<This message was edited>", "").strip()
            

            if msg == "<Media omitted>":
                word_data.append((y, m, d, h, min, None, f, l, f"[{msg.replace('<', '').replace('>', '').split()[0].upper()}]"))
                continue

            if msg == "This message was deleted":
                word_data.append((y, m, d, h, min, None, f, l, "[DELETED MESSAGE]"))
                continue

            msg_words = msg.split(" ")

            for word in msg_words:

                w = word.lower()

                if (w in stop_words):
                    continue

                if ("instagram.com" in w):
                    w = "[INSTAGRAM LINK]"
                if ("youtu.be" in w or "youtube.com" in w):
                    w = "[YOUTUBE LINK]"
                if ("tiktok.com" in w):
                    w = "[TIKTOK LINK]"

                
                clean_w = w.replace(".", "").replace(",", "").replace("!", "").replace("?", "")

                if (clean_w != ""):
                    word_data.append((y, m, d, h, min, None, f, l, clean_w))


        message_data.append((y, m, d, h, min, None, f, l, msg))        



def apple_parsing():

    for line in lines:

        if not (line.startswith("[") or line.startswith("\u200e[")):
            continue
        if "] " not in line:
            continue

        if ("changed the group name" in line):
            continue

        
        timestamp_part, message_part = line.split("] ", 1)

        timestamp_part = timestamp_part.replace("[", "")

        if (":" not in message_part):
            continue



        timestamp_part = clean_unicode(timestamp_part)

        dt = datetime.datetime.strptime(timestamp_part, "%m/%d/%y, %I:%M:%S %p")

        y = dt.year
        m = dt.month
        d = dt.day
        h = dt.hour
        min = dt.minute
        s = dt.second


        if len(message_part.split(":")) >= 2:
            n, msg = message_part.split(":", 1)
            f, l = n.split(" ", 1)

            msg = msg.replace("\u200e", "").replace("\u202f", "").replace("<This message was edited>", "").strip()
            

            if msg in ["sticker omitted", "image omitted", "video omitted", "GIF omitted"]:
                word_data.append((y, m, d, h, min, s, f, l, f"[{msg.split()[0].upper()}]"))
                continue

            if (msg == "This message was deleted."):
                word_data.append((y, m, d, h, min, s, f, l, "[DELETED MESSAGE]"))
                continue

            msg_words = msg.split(" ")

            for word in msg_words:

                w = word.lower()

                if (w in stop_words):
                    continue

                if ("instagram.com" in w):
                    w = "[INSTAGRAM LINK]"
                if ("youtu.be" in w or "youtube.com" in w):
                    w = "[YOUTUBE LINK]"
                if ("tiktok.com" in w):
                    w = "[TIKTOK LINK]"

                
                clean_w = w.replace(".", "").replace(",", "").replace("!", "").replace("?", "")

                if (clean_w != ""):
                    word_data.append((y, m, d, h, min, s, f, l, clean_w))


        message_data.append((y, m, d, h, min, s, f, l, msg))



# ------------------------------------- SQL DATABASE AND TABLE CREATION AND INSERTIONS ---------------------------------------



def sql_upload():

    init = setup()

    if (init == True):
        apple_parsing()
    else:
        android_parsing()

    try:
        
        # First connection, simply logging in and creating a new database for whatever file is put in

        connection0 = mysql.connector.connect(host = sql_host, user = sql_name, password = sql_password)

        precursor = connection0.cursor()
        precursor.execute(f"drop database if exists {database_name}")
        precursor.execute(f"create database {database_name}")

        if 'connection0' in locals() and connection0.is_connected():
            connection0.close()



        # Second connection for actual database editing

        connection = mysql.connector.connect(host = sql_host, user = sql_name, password = sql_password, database = database_name)

        if connection.is_connected():

            cursor = connection.cursor()

            cursor.execute(f"USE {database_name}")



            # ALL_MESSAGES TABLE (info from python is inserted into data_by_message table)
            
            cursor.execute("CREATE TABLE all_messages (year int, month int, day int, hour int, minute int, second int, first_name varchar(255), last_name varchar(255), message varchar(5000));")
        
            cursor.executemany("INSERT INTO all_messages VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", message_data)
            

            
            # ALL_WORDS TABLE (info from python is inserted into table)

            cursor.execute("CREATE TABLE all_words (year int, month int, day int, hour int, minute int, second int, first_name varchar(255), last_name varchar(255), word varchar(500));")

            cursor.executemany("INSERT INTO all_words (year, month, day, hour, minute, second, first_name, last_name, word) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", word_data)




            # WORD_STATS TABLE (calculated from raw SQL tables)

            cursor.execute("CREATE TABLE word_stats AS SELECT first_name, last_name, word, COUNT(*) AS count FROM all_words GROUP BY first_name, last_name, word;")

            

            # MEMBER_STATS (calculated from raw SQL tables)

            cursor.execute("CREATE TABLE member_stats AS SELECT first_name, last_name, COUNT(*) AS total_messages FROM all_messages GROUP BY first_name, last_name;")


                # Average daily messages
            cursor.execute("ALTER TABLE member_stats ADD average_messages_per_active_day int;")

            cursor.execute("UPDATE member_stats JOIN (SELECT first_name, last_name, AVG(count) as avg_column FROM(SELECT first_name, last_name, COUNT(*) as count FROM all_messages GROUP BY first_name, last_name, year, month, day) AS count_table GROUP BY first_name, last_name) AS avg_table ON member_stats.first_name = avg_table.first_name AND member_stats.last_name = avg_table.last_name SET member_stats.average_messages_per_active_day = avg_table.avg_column;")


                # Most common word
            cursor.execute("ALTER TABLE member_stats ADD favorite_word varchar(255)")

            cursor.execute("UPDATE member_stats ms JOIN (SELECT wc.first_name, wc.last_name, wc.word FROM word_stats wc JOIN (SELECT first_name, last_name, MAX(count) AS max_count FROM word_stats GROUP BY first_name, last_name) m ON wc.first_name = m.first_name AND wc.last_name = m.last_name AND wc.count = m.max_count) t ON ms.first_name = t.first_name AND ms.last_name = t.last_name SET ms.favorite_word = t.word;")


                # Most active date
            cursor.execute("ALTER TABLE member_stats ADD most_active_date_year int, ADD most_active_date_month int, ADD most_active_date_day int;")

            cursor.execute("UPDATE member_stats ms JOIN (SELECT d.first_name, d.last_name, d.year, d.month, d.day FROM (SELECT first_name, last_name, year, month, day, COUNT(*) AS daily_count FROM all_messages GROUP BY first_name, last_name, year, month, day) AS d JOIN (SELECT first_name, last_name, MAX(daily_count) AS max_count FROM (SELECT first_name, last_name, year, month, day, COUNT(*) AS daily_count FROM all_messages GROUP BY first_name, last_name, year, month, day) AS daily_counts GROUP BY first_name, last_name) AS m ON d.first_name = m.first_name AND d.last_name = m.last_name AND d.daily_count = m.max_count) AS max_dates ON ms.first_name = max_dates.first_name AND ms.last_name = max_dates.last_name SET ms.most_active_date_year = max_dates.year, ms.most_active_date_month = max_dates.month, ms.most_active_date_day = max_dates.day;")



            # DATE_STATS (calculated from raw SQL tables)

            cursor.execute("CREATE TABLE date_stats (year int, month int, day int, message_count int, most_active_member_first varchar(255), most_active_member_last varchar(255), most_used_word varchar(500));")

            cursor.execute("INSERT INTO date_stats (year, month, day) SELECT year, month, day FROM all_messages GROUP BY year, month, day;")

                # adding message counts per date
            cursor.execute("UPDATE date_stats d JOIN (SELECT year, month, day, COUNT(*) AS count FROM all_messages GROUP BY year, month, day) AS c ON d.year = c.year AND d.month = c.month AND d.day = c.day SET message_count = count;")

                # finding most active member on each day
            cursor.execute("UPDATE date_stats d JOIN (SELECT t.year,t.month,t.day,t.first_name,t.last_name FROM (SELECT year,month,day,first_name,last_name,COUNT(*) AS message_count FROM all_messages GROUP BY year,month,day,first_name,last_name) t JOIN (SELECT year,month,day,MAX(message_count) AS max_count FROM (SELECT year,month,day,first_name,last_name,COUNT(*) AS message_count FROM all_messages GROUP BY year,month,day,first_name,last_name) x GROUP BY year,month,day) m ON t.year=m.year AND t.month=m.month AND t.day=m.day AND t.message_count=m.max_count) r ON d.year=r.year AND d.month=r.month AND d.day=r.day SET d.most_active_member_first=r.first_name, d.most_active_member_last=r.last_name;")
            
                # finding most used word on each date

            cursor.execute("UPDATE date_stats d JOIN (SELECT t.year,t.month,t.day,t.word FROM (SELECT year,month,day,word,COUNT(*) AS word_count FROM all_words GROUP BY year,month,day,word) t JOIN (SELECT year,month,day,MAX(word_count) AS max_count FROM (SELECT year,month,day,word,COUNT(*) AS word_count FROM all_words GROUP BY year,month,day,word) x GROUP BY year,month,day) m ON t.year=m.year AND t.month=m.month AND t.day=m.day AND t.word_count=m.max_count) r ON d.year=r.year AND d.month=r.month AND d.day=r.day SET d.most_used_word=r.word;")
            
            

            connection.commit()



            # pandas DataFrame creation from SQL Tables for visualization in tkinter

            for i in range(len(all_tables)):
                cursor.execute(f"SELECT * FROM {all_tables[i]};")
                results = cursor.fetchall()
                column_names = [i[0] for i in cursor.description]
                data_frames.append(pandas.DataFrame(results, columns=column_names))

            

            
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            return 1
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            return 2
        else:
            return err


    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            return 0



# ------------------------------------- SIMPLE USER INTERFACE WITH TKINTER ---------------------------------------



def select_file():
    global user_file
    user_file = filedialog.askopenfilename(filetypes = [('text files', '*.txt'),])



def on_button_press():

    global sql_host, sql_name, sql_password

    sql_host = e1.get()
    sql_name = e2.get()
    sql_password = e3.get()

    connection_status = sql_upload()

    window2 = tk.Toplevel(screen)
    window2.title("Upload Status")
    window2.geometry("500x200")

    if connection_status == 0:
        confirmation_text = tk.Label(window2, text = "Successfully uploaded chat data into MySQL database.", font = ("Arial", 10, "bold"))
        confirmation_text.pack(expand = True, anchor = "center", pady = 2)

    elif connection_status == 1:
        confirmation_text = tk.Label(window2, text = "Error: something is wrong with your user name or password.", font = ("Arial", 10, "bold"))
        confirmation_text.pack(expand = True, anchor = "center", pady = 2)

    elif connection_status == 2:
        confirmation_text = tk.Label(window2, text = "Error: database does not exist.", font = ("Arial", 10, "bold"))
        confirmation_text.pack(expand = True, anchor = "center", pady = 2)

    else:
        confirmation_text = tk.Label(window2, text = f"Failed to upload chat data.\n{connection_status}", font = ("Arial", 8, "bold"))
        confirmation_text.pack(expand = True, anchor = "center", pady = 2)


    def on_table_button_press():
        
        table_window = tk.Toplevel(screen)

        subheader = tk.Label(table_window, text = "Adjust column sizes by clicking and dragging. Scroll to view more rows.", font = ("Arial", 10, "bold"))
        subheader.pack(anchor = "center", pady = 10)

        table_window.title("Data Tables")
        table_window.geometry("800x800")

        for i in range(len(data_frames)):

            title = tk.Label(table_window, text = f"{all_tables[i]}", font = ("Arial", 8))
            title.pack(anchor = "center", pady = 3)

            tree = ttk.Treeview(table_window, columns = list(data_frames[i].columns), show = "headings")
            tree.pack(fill = "both", pady = 3, expand = True)

            vsb = ttk.Scrollbar(tree, orient = "vertical", command = tree.yview)
            hsb = ttk.Scrollbar(tree, orient = "horizontal", command = tree.xview)
            tree.configure(yscrollcommand = vsb.set, xscrollcommand = hsb.set)

            vsb.pack(side = "right", fill = "y")
            hsb.pack(side = "bottom", fill = "x")


            for col in data_frames[i].columns:
                tree.heading(col, text = col)
                tree.column(col, width = 80, stretch = False)

            for _, row in data_frames[i].iterrows():
                tree.insert("", "end", values = list(row))

        
    view_tables_button = tk.Button(window2, text = "View Tables", command = on_table_button_press)
    view_tables_button.pack(anchor = 'center', pady = 10)



screen = tk.Tk()

screen.title("Text to MySQL Pipeline")


screen.geometry("600x400")
screen.resizable = (False, False)


header = tk.Label(text = "WhatsApp to MySQL Pipeline", font = ("Arial", 20, "bold"))
header.pack(anchor = "center", pady = 15)

subheader = tk.Label(text = "Enter the credentials of the MySQL server you want to store the data in.\nThen, export your WhatsApp Chat as a .txt file and upload it below.\n", font = ("Arial", 10, "bold"))
subheader.pack(anchor = "center", pady = 0)


l1 = tk.Label(text = "Server Name")
l1.pack(anchor = "center", pady = 3)

e1 = tk.Entry()
e1.pack(anchor = "center", pady = 3)


l2 = tk.Label(text = "Username")
l2.pack(anchor = "center", pady = 3)

e2 = tk.Entry()
e2.pack(anchor = "center", pady = 3)


l3 = tk.Label(text = "Password")
l3.pack(anchor = "center", pady = 3)

e3 = tk.Entry(show = "*")
e3.pack(anchor = "center", pady = 3)


file_button = tk.Button(text = "Select File", command = select_file)
file_button.pack(anchor = "center", pady = 20)


connect_button = tk.Button(text = "Connect and Upload", command = on_button_press)
connect_button.pack(anchor = "center", pady = 5)



tk.mainloop()