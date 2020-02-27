#!/usr/bin/env python
'''
Created on May 29, 2017

@author: xizhouf
'''
import os
import shutil
import logging
import glob
import argparse

logpath = "."
logfile = os.path.join(logpath, "usermerge.log")
 
logger = logging.getLogger("account_merge")
logger.setLevel(logging.INFO)
logFormatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
fh = logging.FileHandler(os.path.join(logpath, logfile))
fh.setLevel(logging.INFO)
fh.setFormatter(logFormatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(logFormatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


class User(object):
    """The User class specifies the attributes of a user object.

    A user object has the following attributes:
        username: a string, the user name of the user.
        uid: a integer, the UID for the user. A non-system user has a unique UID across the cluster. This might not be true for a
            system user.
        gid: a integer, the primary GID for the user.
        groups: a list of GIDs which the user belongs to.
    """
    def __init__(self, username, uid, gid):
        self.username = username
        self.uid = int(uid)
        self.gid = int(gid)
        self.groups = [gid]
        
    def __hash__(self):
        return hash(self.username, self.uid, self.gid)
    
    def __eq__(self, other):
        return (self.username, self.uid, self.gid) == (other.username, other.uid, other.gid)


class Group(object):
    """The group class specifies the attributes of a group object.

    A group object has the following attributes:
        groupname: a string, the name of the group
        gid: a integer, teh GID of the group
        users: a list of users
    """
    def __init__(self, groupname, gid):
        self.groupname = groupname
        self.gid = int(gid)
        self.users = []
    
    def __hash__(self):
        return hash(self.groupname, self.gid)
    
    def __eq__(self, other):
        return hash(self.groupname, self.gid) == (other.groupname, other.gid)


def touchFile(path):
    """touchFile create a file if the file doen's exist.

    Args:
        path: a string, the path of the file to be created.

    Returns:
        None.
    """
    basedir = os.path.dirname(path)
    if not os.path.exists(basedir):
        if basedir != "":
            os.makedirs(basedir)
    
    with open(path, 'a'):
        os.utime(path, None)


class UserDB(object):
    """UserDB stores the user and group information that maps to the three auth files in Linux.
    """
    def __init__(self, label, passwd_file, group_file, shadow_file):
        """Initializer of an UserDB object

        Args:
            label: a string, the name the UserDB object.
            passwd_file: the passwd_file that the UserDB object links to.
            group_file: the group_file that the UserDB object links to.
            shadow_file: the group_file that the UserDB object links to.
        """
        logger.info("initializing UserDB db={0} passwd={1} group={2} shadow={3}".format(label, passwd_file, group_file, shadow_file))
        
        self.label = label
    
        self.passwd_file = passwd_file
        self.group_file = group_file
        self.shadow_file = shadow_file

        self.users = {}
        self.groups = {}
        self.shadows = {}
        self.uidMaps = {}
        self.gidMaps = {}

        # Touch the passwd and group files in case the file doesn't exist.
        touchFile(passwd_file)
        touchFile(group_file)

        # Load the users from the passwd file
        nusers = 0
        with open(self.passwd_file) as f:
            for line in f.readlines():
                line = line.rstrip()
                if line.startswith('#') or len(line) == 0:
                    continue
                
                fields = line.split(':')
                if len(fields) >=7:
                    u = User(fields[0], fields[2], fields[3])
                    # If in a single password file, there are duplicated copies of a specific user. We only take the first one.
                    # This situation may never happens in real world because Linux will detect a duplication error.
                    if u.username in self.users.keys():
                        logger.warn("duplicate username username={} uid={}; ignore user entry.".format(u.username, u.uid))
                    else:
                        self.users[u.username] = (u, line)

                    # Similarly, there are may be duplicate UIDs. But it will not happens in real world.
                    if u.uid in self.uidMaps.keys():
                        logger.warn("duplicate uid username={} uid={}; ignore user entry.".format(u.username, u.uid))
                    else:
                        self.uidMaps[u.uid] = u.username

                    nusers = nusers + 1

        # Load the shadow file, we use a map to map a username to a line in the shadow file
        nshadows = 0
        with open(self.shadow_file) as f:
            for line in f.readlines():
                line = line.rstrip()
                # We skip the first line
                if line.startswith('#') or len(line) == 0:
                    continue
                fields = line.split(':')
                if len(fields) >= 9:
                    username = fields[0]
                    self.shadows[username] = line
                    nshadows = nshadows + 1

        # Load the group file.
        ngroups=0
        with open(self.group_file) as f:
            for line in f.readlines():
                line = line.rstrip()
                if line.startswith('#') or len(line) == 0:
                    continue
                
                fields = line.split(':')
                if len(fields) >= 4:
                    g = Group(fields[0], fields[2])

                    g.users = fields[3].split(',')

                    if g.groupname in self.groups.keys():
                        logger.warn("duplicate groupname group={} gid={}; ignore group entry".format(g.groupname, g.gid))
                    else:
                        self.groups[g.groupname] = (g, line)

                    # There is a problem here. On Palmetto, the groups cuuser and cuuser2 have the same GID.
                    if g.gid in self.gidMaps.keys():
                        logger.warn("duplicate gid group={} gid={}; ignore group entry".format(g.groupname, g.gid))
                    else:
                        self.gidMaps[g.gid] = g.groupname

                    for u in g.users:
                        if u == "":
                            continue
                        if u in self.users.keys():
                            self.users[u][0].groups.append(g.gid)
                        else:
                            logger.warn("unknown user ({} in group {})".format(u, g.groupname))

                    ngroups = ngroups + 1
        
        logger.info("finished initializing UserDB db={0} num_users={1} num_shadows={2} num_groups={2}".format(label, nusers, nshadows, ngroups))

    def dump(self):
        print(self.users)
        print(self.groups)
        print(self.gidMaps)

    def addUser(self, user, passwd_entry, shadow_entry):
        """Add userto UserDB.
        
        Note:
            We consider four cases when add a user to UserDB.
            Case 1: user in UserDB with same UID      => update user info and shadow info
            Case 2: user in userDB with different UID => potential conflict, warn and do nothing
            Case 3: both user and UID not in UserDB   => add use as a new user
            Case 4: user not in UserDB but UID in UserID => potential conflict, warn and do nothing
        
        Args:
            user: a user object
            passwd_entry: a string representation of the the passwd entry
            shadow_entry: a string representation of the shadow entry
        
        Returns:
            True if falling into case 1 and 3, False otherwise
        """
        logger.info("trying to add username={0} uid={1}".format(user.username, user.uid))
        if user.username in self.users:
            existing_user = self.users[user.username][0]
            if existing_user.uid == user.uid:
                if user.uid >= 1000:
                    logger.info("updating user username={} uid={}".format(user.username, user.uid))
                    self.users[user.username] = (user, passwd_entry)
                    self.shadows[user.username] = shadow_entry
                    return True
                else:
                    logger.warn("skipping update user username={} uid={}; reason=11-existing user and existing uid".format(user.username, user.uid))
                    #self.users[user.username] = (user, passwd_entry)
                    #self.shadows[user.username] = shadow_entry
                    return False
            else:
                # same username with different UID
                logger.warn("failed to add user username={} uid={}; reason=10-existing user and new uid".format(user.username, user.uid))
                return False
        else:
            if user.uid in self.uidMaps:
                # new username with existing UID
                logger.warn("failed to add user username={} uid={}; reason=10-new user and existing uid".format(user.username, user.uid))
                return False
            else:
                # New username
                self.users[user.username] = (user, passwd_entry)
                self.uidMaps[user.uid] = user.username
                self.shadows[user.username] = shadow_entry
                logger.warn("succeeded to add user username={} uid={}; status=00-new user and new uid".format(user.username, user.uid))
                return True
    
    def addGroup(self, group, group_entry):
        """Add a group to the UserDB
        
        Note:
            We consider four cases when adding a group.
            Case 1: group in UserDB with the same GID      => update group info
            Case 2: group in UserDB with a different GID   => a potential conflict, warn and do nothing
            Case 3: group and GID not in UserDB            => add a new group
            Case 4: group not in UserDB, but GID in UserDB => Potential conflict, warn and do nothing
            
            There is a potential conflicts here because on Palmetto both cuuser and cuuser2 have GID=1000.
            This program ignores this exception because a normal user will have cuuser as its default group
            and there is no difference whether the user is listed on either cuuser and cuuser2 group.   
             
        Args:
            group: a group object
            group_entry: a string representation of the group entry
            
        Returns:
            True if the group is added to UserDB successfully, False otherwise.
        
        """
        if group.groupname in self.groups:
            existing_group = self.groups[group.groupname][0]
            if existing_group.gid == group.gid:
                # Update group with new users
                logger.warn("skipping existing group group={} gid={}".format(group.groupname, group.gid))
                existing_group.users = group.users
                self.groups[group.groupname] = (existing_group, group_entry)
                return True
            else:
                # Group has a new GID
                logger.warn("changing gid for existing group group={} gid={}; not permitted".format(group.groupname, group.gid))
                return False
        else:
            # a new group with an existing GID, we consider the cuuser and cuuser2 exception.
            if group.gid in self.gidMaps:
                if group.groupname == "cuuser" or group.groupname == "cuuser2":
                    gname = self.gidMaps[group.gid]
                    g = self.groups[gname]
                    fields = g[1].split(':')
                    self.groups[gname][0].users.extend(group.users)
                    fields[3] = ','.join(self.groups[gname][0].users)
                    entry = ":".join(fields)
                    self.groups[gname] = (group, entry)
                    return True
                else:
                    logger.warn("assigning a new gid to a new group group={} gid={}; not permitted".format(group.groupname, group.gid))
                    return False
            else:
                # Add a new group
                logger.warn("adding a new group with a new gid group={} gid={}; succeeded".format(group.groupname, group.gid))
                self.groups[group.groupname] = (group, group_entry)
                self.gidMaps[group.gid] = group.groupname               
                return True
    
    def addUserByPasswdEntry(self, passwd_entry):
        fields = passwd_entry.split(':')
        if len(fields) >=7:
            user = User(fields[0], fields[2], fields[3])
            return self.addUser(user, passwd_entry)
        else:
            print("Wring passwd entry format: {}".format(passwd_entry))
            return False
    
    def save(self):
        """Save the UserDB to the linked Files

        Returns:
            None
        """
        tmpFile = self.passwd_file.split(".")[0] + ".merged"
        with open(tmpFile, "w+") as f:
            uids_numerical = [u for u in self.uidMaps.keys()]
            uids_string = [str(i) for i in sorted(uids_numerical)]
            for uid in uids_string:
                u = self.uidMaps[int(uid)]
                f.write(self.users[u][1])
                f.write('\n')
        #shutil.copyfile(tmpFile, self.passwd_file) 
        
        tmpFile = self.shadow_file.split(".")[0] + ".merged"
        with open(tmpFile, "w+") as f:
            uids_numerical = [u for u in self.uidMaps.keys()]
            uids_string = [str(i) for i in sorted(uids_numerical)]
            for uid in uids_string:
                u = self.uidMaps[int(uid)]
                if u != None and u in self.shadows.keys():
                   f.write(self.shadows[u])
                   f.write('\n')
        #shutil.copyfile(tmpFile, self.shadow_file) 

        tmpFile = self.group_file.split(".")[0] + ".merged"
        with open(tmpFile, "w+") as f:
            gids_numerical = [g for g in self.gidMaps.keys()]
            gids_string = [str(i) for i in sorted(gids_numerical)]
            
            for gid in gids_string:
                g = self.gidMaps[int(gid)]
                f.write(self.groups[g][1])
                f.write('\n')
        #shutil.copyfile(tmpFile, self.group_file) 
    
    def mergeDB(self, other):
        logger.info("merging userDB {0} with {1}".format(self.label, other.label))
        
        for u in other.users.keys():
            user = other.users[u][0]
            passwd_entry = other.users[u][1]
            if u in other.shadows.keys():
                shadow_entry = other.shadows[u]
                self.addUser(user, passwd_entry, shadow_entry)
            else:
                logger.error("no shadow entry for user username={}".format(u))
                continue
        
        for g in other.groups.keys():
            group = other.groups[g][0]
            group_entry = other.groups[g][1]
            self.addGroup(group, group_entry)


def getUpdateFlag(workdir):
    path = os.path.join(workdir, "flag")
    flag = 0
    if os.path.exists(path):
        with open(path) as f:
            flag = int(f.readline())
    return flag


def setUpdateFlag(workdir, flag):
    path = os.path.join(workdir, "flag")
    with open(path, 'w+') as f:
        f.write(str(flag))


def main(args):
    """The main function that keep accounts in sync.


    Note: 
        This function assumes that the master node will put the master copy of user accounts files 
        into /root/accounts/[yyyymmdd].
        
        To avoid redundant sync, the program use a flag in each update directory to indicate if 
        there is a need to process the data.
        flag = 1 means update is needs; flag = 2 means already processed. 
        
        A cron job will call his function periodically to update local accounts.
    
    Returns:
        None
    """
    
    # switch to work directory
    try:
        os.chdir(args.workdir)
    except Exception as e:
        logger.error("errors in changing directory to {} - {}".format(args.workdir, e))
        return
    logger.info("switched into working directory - {}".format(args.workdir))

    try:
        for fn in ['passwd', 'group', 'shadow']:
            shutil.copyfile('/etc/{}'.format(fn), 'local_{}.orig'.format(fn))
            logger.info('copied account file - {}'.format(fn))
    except Exception as e:
        logger.error("errors in copying local files - {}".format(e))
        return
    logger.info("copied account files into working directory")

    # load local accounts to UserDB
    localDB = UserDB("Local DB", "local_passwd.orig", "local_group.orig", "local_shadow.orig")
    
    # process each update
    basedir = args.workdir
    dirs = glob.glob("*")
    for d in dirs:
        work_dir = os.path.join(basedir, d)
        if not os.path.isdir(work_dir):
            continue

        flag = getUpdateFlag(work_dir)
        if flag == 2:
            continue
        
        updateDB = UserDB("Updates-{0}".format(d), os.path.join(work_dir, args.master_passwd_file),
                          os.path.join(work_dir, args.master_group_file),
                          os.path.join(work_dir, args.master_shadow_file))
        localDB.mergeDB(updateDB)
        localDB.save()
        #setUpdateFlag(work_dir, 2)
        logger.info("merged account file with account files in {}".format(work_dir))

    if not args.update: 
        logger.info("local account was not updated per user request; done working.") 
        return
    logger

    try: 
        # copy data back to /etc 
        for fn in ['passwd', 'group', 'shadow']:
            shutil.copyfile('local_{}.merged'.format(fn), '/etc/{}'.format(fn)) 
    except Exception as e: 
        logger.error("errors in updating local files - {}".format(e)) 
        return
    logger.info("done merging")


if __name__ == '__main__':
    p = argparse.ArgumentParser("Merge accounts on master into local account")
    p.add_argument("--workdir", default="/root/accounts", help="working directory")
    p.add_argument("--master_passwd_file",  default="passwd", help="the file that contains the accounts on master")
    p.add_argument("--master_group_file", default="group", help="the file that contains the accounts on master")
    p.add_argument("--master_shadow_file", default="shadow", help="the file that contains the shadow on master")
    p.add_argument("--update", default=True, action="store_true", help="whether updating the local accounts")
    args = p.parse_args()

    logger.info("start merging accounts") 
    main(args)
