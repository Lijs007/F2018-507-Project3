import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def get_country_fk(country_name):
    try:
        statement = '''
            SELECT Id
            FROM Countries c
            WHERE c.EnglishName = (?)
        '''
        cur.execute(statement, [country_name])
        return int(cur.fetchone()[0])
    except:
        return None

conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

statement = '''
    DROP TABLE IF EXISTS 'Bars';
'''
cur.execute(statement)

statement = '''
    DROP TABLE IF EXISTS 'Countries';
'''
cur.execute(statement)

statement = '''
    CREATE TABLE 'Bars' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Company' TEXT NOT NULL,
        'SpecificBeanBarName' TEXT NOT NULL,
        'REF' TEXT NOT NULL,
        'ReviewDate' TEXT NOT NULL,
        'CocoaPercent' REAL NOT NULL,
        'CompanyLocationId' INTEGER NOT NULL,
        'Rating' REAL NOT NULL,
        'BeanType' TEXT,
        'BroadBeanOriginId' INTEGER NULL
    );
'''
cur.execute(statement)

statement = '''
    CREATE TABLE 'Countries' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Alpha2' TEXT NOT NULL,
        'Alpha3' TEXT NOT NULL,
        'EnglishName' TEXT NOT NULL,
        'Region' TEXT NOT NULL,
        'Subregion' TEXT NOT NULL,
        'Population' INTEGER NOT NULL,
        'Area' REAL NULL
    );
'''
cur.execute(statement)

conn.commit()

fr = open(COUNTRIESJSON, 'r', encoding='utf-8')
contents = fr.read()
fr.close()
countries_diction = json.loads(contents)

for country in countries_diction:
    insertion = (None, country['alpha2Code'], country['alpha3Code'], country['name'], country['region'], country['subregion'], country['population'], country['area'])
    statement = 'INSERT INTO "Countries" '
    statement += "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    cur.execute(statement, insertion)

conn.commit()

with open(BARSCSV, 'r', encoding='utf-8') as bars:
    reader = csv.reader(bars)
    flag = False
    for row in reader:
        if flag:
            insertion = (None, row[0], row[1], row[2], row[3], float(row[4][:-1])/100,  get_country_fk(row[5]),  row[6],  row[7],  get_country_fk(row[8]))
            statement = 'INSERT INTO "Bars" '
            statement += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cur.execute(statement, insertion)
        else:
            flag = True


conn.commit()
conn.close()


# Part 2: Implement logic to process user commands
def process_command(command):
    commands = command.split()
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    if commands[0] == 'bars':
        order_by = 'b.Rating'
        where = ''
        limit = 'DESC LIMIT 10'
        for i in range(1,len(commands)):
            if commands[i] == 'cocoa':
                order_by = 'b.CocoaPercent'
            elif commands[i] == 'ratings': 
                pass
            elif '=' in commands[i]:
                eqlist = commands[i].split('=')
                if eqlist[0] == 'sellcountry':
                    where = 'WHERE {} = "{}"'.format('c1.Alpha2' , eqlist[1])
                elif eqlist[0] == 'sourcecountry':
                    where = 'WHERE {} = "{}"'.format('c2.Alpha2' , eqlist[1])
                elif eqlist[0] == 'sellregion':
                    where = 'WHERE {} = "{}"'.format('c1.region' , eqlist[1])
                elif eqlist[0] == 'sourceregion':
                    where = 'WHERE {} = "{}"'.format('c2.region', eqlist[1])
                elif eqlist[0] == 'top':
                    limit = 'DESC LIMIT {}'.format(eqlist[1])
                elif eqlist[0] == 'bottom':
                    limit = 'LIMIT {}'.format(eqlist[1])
                else:
                    #print('Command not recognized: ' + command)
                    conn.close()
                    return -1
            else:
                conn.close()
                return -1

        statement = '''
            SELECT b.SpecificBeanBarName, b.Company, c1.EnglishName, b.Rating, b.CocoaPercent, c2.EnglishName
            FROM bars b
            LEFT JOIN countries c1
            ON b.CompanyLocationId = c1.Id
            LEFT JOIN countries c2
            ON b.BroadBeanOriginId = c2.Id
        '''
        statement += where
        statement += ' ORDER BY ' + order_by + ' '
        statement += limit

    elif commands[0] == 'companies':
        order_by = 'AVG(Rating)'
        where = ''
        limit = 'DESC LIMIT 10'
        group_by = 'GROUP BY Company'
        agg = 'AVG(Rating)'
        for i in range(1,len(commands)):
            if commands[i] == 'cocoa':
                order_by = 'AVG(CocoaPercent)'
                agg = 'AVG(CocoaPercent)'
            elif commands[i] == 'ratings': 
                pass
            elif commands[i] == 'bars_sold':
                order_by = 'COUNT(*)'
                agg = 'COUNT(*)'
            elif '=' in commands[i]:
                eqlist = commands[i].split('=')
                if eqlist[0] == 'country':
                    where += 'WHERE {} = "{}"'.format('c.Alpha2', eqlist[1])
                elif eqlist[0] == 'region':
                    where += 'WHERE {} = "{}"'.format('c.Region' , eqlist[1])
                elif eqlist[0] == 'top':
                    limit = 'DESC LIMIT {}'.format(eqlist[1])
                elif eqlist[0] == 'bottom':
                    limit = 'LIMIT {}'.format(eqlist[1])
                else:
                    #print('Command not recognized: ' + command)
                    conn.close()
                    return -1
            else:
                conn.close()
                return -1
        statement = 'SELECT b.Company, b.CompanyLocationId, ' + agg + ' FROM bars b'
        statement += ' ' + 'JOIN countries c ON c.Id = b.CompanyLocationId'
        statement += ' ' + where
        statement += ' ' + group_by
        statement += ' ' + 'HAVING COUNT(*) > 4'
        statement += ' ORDER BY ' + order_by + ' '
        statement += limit

    elif commands[0] == 'countries':
        order_by = 'AVG(Rating)'
        where = ''
        limit = 'DESC LIMIT 10'
        group_by = 'GROUP BY c.EnglishName'
        agg = 'AVG(Rating)'
        on = ' ON c.Id = b.CompanyLocationId'
        for i in range(1,len(commands)):
            if commands[i] == 'cocoa':
                order_by = 'AVG(CocoaPercent)'
                agg = 'AVG(CocoaPercent)'
            elif commands[i] == 'ratings': 
                pass
            elif commands[i] == 'bars_sold':
                order_by = 'COUNT(*)'
                agg = 'COUNT(*)'
            elif commands[i] == 'sellers':
                on = ' ON c.Id = b.CompanyLocationId'
            elif commands[i] == 'sources':
                on = ' ON c.Id = b.BroadBeanOriginId'
            elif '=' in commands[i]:
                eqlist = commands[i].split('=')
                if eqlist[0] == 'region':
                    where += 'WHERE {} = "{}"'.format('c.Region', eqlist[1])
                elif eqlist[0] == 'top':
                    limit = 'DESC LIMIT {}'.format(eqlist[1])
                elif eqlist[0] == 'bottom':
                    limit = 'LIMIT {}'.format(eqlist[1])
                else:
                    #print('Command not recognized: ' + command)
                    conn.close()
                    return -1
            else:
                conn.close()
                return -1
        statement = 'SELECT c.EnglishName, c.Region, ' + agg + ' FROM bars b'
        statement += ' ' + 'JOIN countries c' + on
        statement += ' ' + where
        statement += ' ' + group_by
        statement += ' ' + 'HAVING COUNT(*) > 4'
        statement += ' ORDER BY ' + order_by + ' '
        statement += limit

    elif commands[0] == 'regions':
        order_by = 'AVG(Rating)'
        where = ''
        limit = 'DESC LIMIT 10'
        group_by = 'GROUP BY c.Region'
        agg = 'AVG(Rating)'
        on = ' ON c.Id = b.CompanyLocationId'
        for i in range(1,len(commands)):
            if commands[i] == 'cocoa':
                order_by = 'AVG(CocoaPercent)'
                agg = 'AVG(CocoaPercent)'
            elif commands[i] == 'ratings': 
                pass
            elif commands[i] == 'bars_sold':
                order_by = 'COUNT(*)'
                agg = 'COUNT(*)'
            elif commands[i] == 'sellers':
                on = ' ON c.Id = b.CompanyLocationId'
            elif commands[i] == 'sources':
                on = ' ON c.Id = b.BroadBeanOriginId'
            elif '=' in commands[i]:
                eqlist = commands[i].split('=')
                if eqlist[0] == 'top':
                    limit = 'DESC LIMIT {}'.format(eqlist[1])
                elif eqlist[0] == 'bottom':
                    limit = 'LIMIT {}'.format(eqlist[1])
                else:
                    #print('Command not recognized: ' + command)
                    conn.close()
                    return -1
            else:
                conn.close()
                return -1

        statement = 'SELECT c.Region, ' + agg + ' FROM bars b'
        statement += ' ' + 'JOIN countries c' + on
        statement += ' ' + where
        statement += ' ' + group_by
        statement += ' ' + 'HAVING COUNT(*) > 4'
        statement += ' ORDER BY ' + order_by + ' '
        statement += limit
    else:
        conn.close()
        return -1


    #print(statement)
    try:
        cur.execute(statement)
        tuples = cur.fetchall()
    except:
        tuples = -1
    conn.close()
    #r_tuples = []
    #if commands[0] == 'bars':
     #   for tup in tuples:
      #      r_tuples.append(tup[:2] + (get_name_from_id(tup[2]),) + tup[3:5] + (get_name_from_id(tup[5]),))
    #else:
     #   r_tuples = tuples
    return tuples


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    while True:
        response = input('Enter a command: ')
        if response == 'exit':
            print('bye')
            break
        elif response == 'help':
            print(help_text)
            continue
        else:
            tuples = process_command(response)
            #print(tuples)
            if tuples == -1:
                print('Command not recognized: ' + response)
            else:
                for tup in tuples:
                    line = ''
                    for i in range(len(tup)):
                        if tup[i] == None:
                            item = 'Unknown'
                        elif response.split()[0] == 'bars' and i == 4:
                            item = str(tup[i]*100) + '%'
                        else:
                            item = str(tup[i])
                        line += '{0:16}'.format(item[0:12] + '...' if len(item) > 12 else item)
                    print(line)
        print('')

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
