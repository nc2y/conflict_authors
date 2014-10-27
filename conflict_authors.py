# -*- coding: utf-8 -*-
from __future__ import print_function
"""
Nicolas Christin
nicolasc@cmu.edu

10/23/2014

conflict_authors.py:
    Extracts a list of authors from HotCRP database, and performs a lookup in
    DBLP to figure out who were their coauthors. This generates a list of
    conflicts, which are then checked to see if they were marked as conflicts
    in HotCRP and if they are in the PC. 

    Tweaks and improvements welcome!
"""

import os 
import sys
import sqlite3
import MySQLdb
import simplejson
import urllib
import re
from config import *

reload(sys)
sys.setdefaultencoding('utf-8') # really, really ugly hack

# constants and global variables
DEBUG = 0

db = MySQLdb.connect(host = db_host,
                     user = user,
                     passwd = passwd, 
                     db = db_name,
                     use_unicode = True,
                     charset = 'utf8')

def warning(*objs):
    print(*objs, file=sys.stderr)

def latin_to_ascii(name):
     
    xlate={0xc0:'A', 0xc1:'A', 0xc2:'A', 0xc3:'A', 0xc4:'A', 0xc5:'A', 
           0xc6:'Ae', 0xc7:'C',
           0xc8:'E', 0xc9:'E', 0xca:'E', 0xcb:'E',
           0xcc:'I', 0xcd:'I', 0xce:'I', 0xcf:'I',
           0xd0:'Th', 0xd1:'N',
           0xd2:'O', 0xd3:'O', 0xd4:'O', 0xd5:'O', 0xd6:'O', 0xd8:'O',
           0xd9:'U', 0xda:'U', 0xdb:'U', 0xdc:'U',
           0xdd:'Y', 0xde:'th', 0xdf:'ss',
           0xe0:'a', 0xe1:'a', 0xe2:'a', 0xe3:'a', 0xe4:'a', 0xe5:'a',
           0xe6:'ae', 0xe7:'c',
           0xe8:'e', 0xe9:'e', 0xea:'e', 0xeb:'e',
           0xec:'i', 0xed:'i', 0xee:'i', 0xef:'i',
           0xf0:'th', 0xf1:'n',
           0xf2:'o', 0xf3:'o', 0xf4:'o', 0xf5:'o', 0xf6:'o', 0xf8:'o',
           0xf9:'u', 0xfa:'u', 0xfb:'u', 0xfc:'u',
           0xfd:'y', 0xfe:'th', 0xff:'y',
           0xa1:'!', 0xa2:'{cent}', 0xa3:'{pound}', 0xa4:'{currency}',
           0xa5:'{yen}', 0xa6:'|', 0xa7:'{section}', 0xa8:'{umlaut}',
           0xa9:'{C}', 0xaa:'{^a}', 0xab:'<<', 0xac:'{not}',
           0xad:'-', 0xae:'{R}', 0xaf:'_', 0xb0:'{degrees}',
           0xb1:'{+/-}', 0xb2:'{^2}', 0xb3:'{^3}', 0xb4:"'",
           0xb5:'{micro}', 0xb6:'{paragraph}', 0xb7:'*',
           0xb8:'{cedilla}',
           0xb9:'{^1}', 0xba:'{^o}', 0xbb:'>>', 
           0xbc:'{1/4}', 0xbd:'{1/2}', 0xbe:'{3/4}', 0xbf:'?',
           0xd7:'*', 0xf7:'/'}

    nonasciire = re.compile(u'([\x00-\x7f]+)|([^\x00-\x7f])', re.UNICODE).sub
    return str(nonasciire(lambda x: x.group(1) or
                          xlate.setdefault(ord(x.group(2)), ''), name))

def get_dblp_conflicts(author):
    canon_author = author.lower().decode("utf-8").replace(" ",
                                                          "_").encode("utf-8")
    coauthors = []
    r1 = re.compile(r"[0-9]")
    r2 = re.compile(r" *$")

    for year in CONFLICT_YEARS:
        dblp_url="http://www.dblp.org/search/api/?q=ce:year:"\
                +year+":*%20ce:author:"+canon_author+\
                ":*&h=1000&c=4&f=0&format=json"

        if (DEBUG): 
            warning("Querying %s" % dblp_url.encode('utf8'))
        dblp_result = simplejson.load(urllib.urlopen(dblp_url))

        try:
            results = dblp_result['result']['hits']['hit']
            for r in results:
                author_list = r['info']['authors']['author']
                for a in author_list:
                    a = r1.sub("", a) # DBLP adds numbers to some authors
                    a = r2.sub("", a) # and trailing spaces to some 
                    if (a != author and a not in coauthors): 
                        coauthors.append(a)
        except KeyError:
            if (DEBUG):
                warning("No paper for %s in %s." % (author, year))

    if (DEBUG): 
        print(author+"'s (recent) conflicts are:")
        for c in coauthors:
            print(c.encode('utf8'))

    return(coauthors)

def split_name(name):
    """
    This functions splits a full name into a first name/last name using the
    heuristic that 1) names are separated by a space, 2) the name in the last
    position is the last name, and 3) first names always start with a capital,
    while 4) last names not in the last position start with a lower case (e.g.,
    "Vincent van Gogh"). It's probably not 100% accurate, but seems good enough
    for now. 
    """

    names = name.split(" ")
    
    if (len(names) == 1):
        first = ""
        last = names[0]
    elif (len(names) == 2):
        first = names[0]
        last = names[1]
    elif (len(names) > 2):
        first = names[0]
        last = ""
        for i in range(1, len(names)):
            if (i < len(names) - 1 and names[i][0].isupper()):
                first = first+" "+names[i]
            else:
                for j in range(i, len(names)):
                    last += names[j]+" "
                break
        last = last[:-1]

    return (first, last)

def is_in_pc(name):
    c = db.cursor()
    [first, last] = split_name(name)

    first = "%"+first+"%"
    last = "%"+last+"%"

    c.execute("SELECT COUNT(*) from ContactInfo WHERE firstName LIKE %s AND\
              lastName LIKE %s AND roles >= 1", (first, last))
    row = c.fetchone()
    return (row[0] > 0)

def is_in_hotcrp_pc_conflicts(name, paper_id):
    """
    Checks if a given name has been listed as a PC conflict in HotCRP
    for a given paper.
    """ 

    c = db.cursor()
    [first, last] = split_name(name)

    first = "%"+first+"%"
    last = "%"+last+"%"
    
    c.execute("SELECT COUNT(*) from PaperConflict, ContactInfo \
              WHERE ContactInfo.firstName LIKE %s AND \
              ContactInfo.lastName LIKE %s AND \
              PaperConflict.paperId=%s AND\
              ContactInfo.contactId=PaperConflict.contactId", 
              (first, last, paper_id))
    row = c.fetchone()

    return (row[0] > 0)

def get_hotcrp_pc_conflicts(paper_id):
    """
    Returns PC conflicts in HotCRP for a given paper_id. 
    """ 

    c = db.cursor()
    conflicts = []
    c.execute("SELECT ContactInfo.firstName, ContactInfo.lastName FROM\
              PaperConflict, ContactInfo WHERE PaperConflict.paperId=%s AND\
              ContactInfo.contactId=PaperConflict.contactId", (paper_id,))

    rows = c.fetchall()
    
    for row in rows:
        name = row[0]+" "+row[1]
        if (is_in_pc(name)):
            conflicts.append(["Individual", name, True, True])

    return conflicts

def get_hotcrp_collab_conflicts(paper_id):
    """
    !UNREACHABLE CODE!

    Returns collaborators in HotCRP for a given paper_id. 
    This function is _never_ called at the moment. It actually does a 
    reasonable job of returning proper collaborator lists, but the problem is
    that HotCRP doesn't seem to use the collaborator field much (or at all?)

    I am leaving it in case we want to do something with it in the future.
    """ 

    c = db.cursor()
    conflicts = []
    
    c.execute("SELECT collaborators from PaperConflict, ContactInfo\
              WHERE PaperConflict.paperId=%s and\
              ContactInfo.contactId=PaperConflict.contactId", (paper_id,))

    conflict_rows = c.fetchone()

    paren_regex = re.compile("\(.*?\)")
    name_regex = re.compile("[A-Z][a-z]+ [A-Z][a-z]+$")
    prof_regex = re.compile("prof[\. ]", re.IGNORECASE)
    dr_regex = re.compile("dr[\. ]", re.IGNORECASE)
    univ_regex = re.compile("university", re.IGNORECASE)
    lab_regex = re.compile("research", re.IGNORECASE)

    """
    Follows a really crappy heuristic: institutional conflicts are the first
    conflicts listed, until we hit one conflict that looks like "Firstname
    LastName" without "university" as a substring, or "FirstName LastName
    (Institution)". I don't know how to do better.
    """

    rows = conflict_rows[0].split("\n")
    institutional = True

    for collaborator in rows:
        if (collaborator is None or collaborator==""):
            continue

        collaborator = collaborator[:-1] # strip out last character

        m1 = paren_regex.search(collaborator)
        m2 = name_regex.match(collaborator)
        m3 = univ_regex.search(collaborator) 
        m4 = lab_regex.search(collaborator) 
        m5 = dr_regex.match(collaborator) 
        m6 = prof_regex.match(collaborator) 

        if m1:
            if (DEBUG):
                warning("Parenthesis match for %s" % collaborator)
            institutional = False
            name = collaborator[:m1.start()-1] # name is before institution
    
            pc = is_in_pc(name)
            listed = is_in_hotcrp_pc_conflicts(name)

            if (["Individual", name, pc, listed] not in conflicts): 
                conflicts.append(["Individual", name, pc, listed])
        elif m5:
            if (DEBUG):
                warning("Dr. match for %s" % collaborator)
            institutional = False
            name = collaborator[m5.end():] # name is after title
            
            pc = is_in_pc(name)
            listed = is_in_hotcrp_pc_conflicts(name)

            if (["Individual", name, pc, listed] not in conflicts):
                conflicts.append(["Individual", name, pc, listed])
        elif m6:
            if (DEBUG):
                warning("Prof. match for %s" % collaborator)
            institutional = False
            name = collaborator[m6.end():] # name is after title
            
            pc = is_in_pc(name)
            listed = is_in_hotcrp_pc_conflicts(name)

            if (["Individual", name, pc, listed] not in conflicts): 
                conflicts.append(["Individual", name, pc, listed])

        elif (m2 and not m3 and not m4):
            if (DEBUG):
                warning("Name structure match for %s" % collaborator)

            institutional = False
            
            pc = is_in_pc(collaborator)
            listed = is_in_hotcrp_pc_conflicts(collaborator)

            if (["Individual", collaborator, pc, listed] not in conflicts): 
                conflicts.append(["Individual", collaborator, pc, listed])

        elif (institutional):
            if (DEBUG):
                warning("No regex match for %s -- institution?" % collaborator)

            if (["Institution", collaborator] not in conflicts):
                conflicts.append(["Institution", collaborator])
        else: 
            if (DEBUG):
                warning("No match for %s -- individual?" % collaborator)
            
            pc = is_in_pc(collaborator)
            listed = is_in_hotcrp_pc_conflicts(collaborator)

            if (["Individual", collaborator, pc, listed] not in conflicts):
                conflicts.append(["Individual", collaborator, pc, listed])
    
    return conflicts

def main(): 
    dblp_conflict_list = {}
    c = db.cursor() 
    c.execute("SELECT paperId, authorInformation from Paper;")
    rows = c.fetchall()

    for row in rows:
        paper_id = row[0]
        authors = row[1]

        print("Processing paper %s..." % paper_id)
        dblp_conflict_list[paper_id] = []
        author_list = authors.decode('utf-8').split('\n')

        for a in author_list:
            first_name = ""
            last_name = ""
            email_address = ""
            institution = ""

            try: 
                author_fields = a.decode('utf-8').split('\t')
                first_name = author_fields[0]
                last_name = author_fields[1]

                author_name = first_name+" "+last_name
                email_address = author_fields[2]
                institution = author_fields[3]
                if (DEBUG): 
                    print("Paper id %d - Author: %s <%s> %s" % (paper_id, 
                                                                author_name, 
                                                                email_address,
                                                                institution))

                if (is_in_pc(author_name)):
                    conflict = ["Individual", author_name, True, True]
                    dblp_conflict_list[paper_id].append(conflict)
                    
                if (["Institution", institution] not in
                    dblp_conflict_list[paper_id]):
                    dblp_conflict_list[paper_id].append(["Institution",
                                                         institution])

                for coauthor in get_dblp_conflicts(author_name):
                    conflict = ["Individual", coauthor, is_in_pc(coauthor), 
                                is_in_hotcrp_pc_conflicts(coauthor, paper_id)]
                    if (conflict not in dblp_conflict_list[paper_id]):
                        dblp_conflict_list[paper_id].append(conflict)

            except IndexError:
                if (DEBUG): 
                    warning("Unparsable author name.")

    for p in dblp_conflict_list.keys():
        print("----------------------------------------------------------------")
        print("Paper %s's discrepancies:" % p)
        
        print("\nPC co-authors according to DBLP, not listed as a conflict:")

        if ([auth for auth in dblp_conflict_list[p] 
             if auth[0] == "Individual" and auth[2] and not auth[3]]): 
            for c in [auth for auth in dblp_conflict_list[p] 
                      if auth[0] == "Individual" and auth[2] and not auth[3]]:
                print("%s might be a PC conflict but not listed as such." %
                      c[1])
        else:
            print("None")

        print("\nHotCRP discrepancies:")
        hc = get_hotcrp_pc_conflicts(p)
        if (hc):
            for c in [x for x in hc if x[0] == "Institution"]:
                if (c not in dblp_conflict_list[p]):
                    print("%s listed as an institutional conflict in HotCRP" 
                          + "not obvious." % c[1])
            for c in [x for x in hc if x[0] == "Individual"]:
                if (c not in dblp_conflict_list[p]):
                    print("%s listed as a PC conflict in HotCRP, not in DBLP."
                          % c[1])
        else:
            print("None")

    db.close()

if __name__ == "__main__":
    main()
