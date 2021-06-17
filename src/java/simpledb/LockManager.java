package simpledb;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {

    private HashMap<PageId, LockMaster>   locks;
    private HashMap<TransactionId, ArrayList<TransactionId>> lockMap;
    private Lock lck;

    public enum WhichLock {
        NONE,
        SHARE,
        EXCLUSIVE,
    }

    public enum WhichPermission {
        READ_ONLY,
        READ_WRITE,
    }

    public LockManager() {
        locks = new HashMap<>();
        lockMap = new HashMap<>();
        lck = new ReentrantLock();

    }

    private void getWhichLock(TransactionId tid, PageId pid, WhichPermission whichPermission) throws TransactionAbortedException {

        switch (whichPermission){
            case READ_ONLY:
                getPublicLock(tid, pid);
                break;
            case READ_WRITE:
                getPrivateLock(tid, pid);
                break;
            default:
                break;
        }
    }

    void getLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {
        LockMaster lockMaster;

        if (!locks.containsKey(pid) || locks.get(pid) == null){
            lockMaster = new LockMaster(lck.newCondition(), lockMap);
            locks.put(pid, lockMaster);
        }
        else {
            lockMaster = locks.get(pid);
        }

        WhichPermission whichPermission = null;
        if (perm == Permissions.READ_WRITE) {
            whichPermission = WhichPermission.READ_WRITE;
        }
        else if (perm == Permissions.READ_ONLY) {
            whichPermission =WhichPermission.READ_ONLY;
        }

        assert whichPermission != null;
        this.getWhichLock(tid, pid, whichPermission);

    }


    private void getPrivateLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        LockMaster locker = locks.get(pid);
        lck.lock();

        if (locker.getPrivateLock(tid) && detectDeadLock(locker, tid)) {
            lck.unlock();
            throw new TransactionAbortedException();
        }
        else if (locker.getPrivateLock(tid) && !detectDeadLock(locker, tid)){
            locker.wait(WhichLock.EXCLUSIVE, tid);
        }

        lck.unlock();
    }


    private void getPublicLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        LockMaster locker = locks.get(pid);
        lck.lock();


        if (locker.getPublicLock(tid) && detectDeadLock(locker, tid)) {

                lck.unlock();
                throw new TransactionAbortedException();
            }
        else if (locker.getPublicLock(tid) && !detectDeadLock(locker, tid)){
            locker.wait(WhichLock.SHARE, tid);
        }

        lck.unlock();
    }

    boolean keepLocking(TransactionId tid, PageId pid) {

        try {
            lck.lock();
            boolean condIsPid = locks.containsKey(pid);
            if (condIsPid) {
                LockMaster lockMaster = locks.get(pid);
                boolean condLCMaster = lockMaster.keepLocking(tid);
                return condLCMaster;
            }
            return false;
        }
        finally {
            lck.unlock();
        }
    }


    void releaseLock(TransactionId tid, PageId pid) {
        lck.lock();

        if (locks.containsKey(pid))
            locks.get(pid).releaseLock(tid);

        lck.unlock();

    }


    void releaseAll(TransactionId tid) {
        lck.lock();

        lockMap.remove(tid);
        for (PageId pageId : locks.keySet()){
            LockMaster lockMaster = locks.get(pageId);
            lockMaster.releaseLock(tid);
        }

        lck.unlock();
    }







    private boolean detectDeadLock(LockMaster locker, TransactionId tid)
    {
        HashSet<TransactionId> visited = new HashSet<>();
        Queue<TransactionId> queue = new LinkedList<>();
        List<TransactionId> transactions;

        for (TransactionId owner: locker.owners) {
            if (!queue.offer(owner)){
                break;
            }
                visited.add(owner);

            while (!queue.isEmpty()) {
                TransactionId id = queue.poll();
                if (lockMap.get(id) == null){
                    continue;
                }
                else {
                    transactions = lockMap.get(id);
                }

                for (TransactionId transactionId : transactions) {
                    if (transactionId.equals(tid)) {
                        return true;
                    }
                    if (!visited.contains(transactionId)) {
                        queue.add(transactionId);
                        visited.add(transactionId);
                    }
                }
                transactions.clear();
            }

            visited.clear();
            queue.clear();
        }
        return false;
    }


    public static class LockMaster {
        final Condition cond;
        LockManager.WhichLock  enumLock;
        HashMap<TransactionId, ArrayList<TransactionId>> mapLock;
        HashSet<TransactionId> owners;
        HashSet<TransactionId> candidates;



        public LockMaster(Condition condition, HashMap<TransactionId, ArrayList<TransactionId>> graph) {
            this.cond = condition;
            this.enumLock = LockManager.WhichLock.NONE;
            this.mapLock = graph;
            this.owners = new HashSet<>();
            this.candidates = new HashSet<>();

        }

        boolean tryGetSharedLock(TransactionId tid) {
            if (enumLock == LockManager.WhichLock.EXCLUSIVE) {
                return !owners.contains(tid);
            }
            return false;
        }

        void convertLock(LockManager.WhichLock whichLock){

            switch (enumLock){
                case NONE:
                    enumLock = LockManager.WhichLock.SHARE;
                    break;
                default:
                    break;
            }

        }

        boolean getPublicLock(TransactionId tid)
                throws TransactionAbortedException  {
            if (tryGetSharedLock(tid)) {
                return true;
            }

            this.convertLock(enumLock);


            if (!owners.contains(tid)) {
                owners.add(tid);
                for (TransactionId waiter: candidates) {
                    ArrayList<TransactionId> a = mapLock.computeIfAbsent(waiter, k -> new ArrayList<>());
                    a.add(tid);
                }
            }
            return false;
        }

        void releaseLock(TransactionId tid) {
            candidates.remove(tid);
            boolean cond1 = owners.contains(tid);
            if (!cond1) {
                return;
            }
            boolean cond2 = enumLock == WhichLock.NONE;
            if (cond2){
                return;
            }

            switch (enumLock){
                case SHARE:
                    owners.remove(tid);
                    if (owners.size() == 0) {
                        enumLock = LockManager.WhichLock.NONE;
                    }
                    break;

                case EXCLUSIVE:
                    if (owners.contains(tid)) {
                        owners.remove(tid);
                        owners.clear();
                    }
                    enumLock = LockManager.WhichLock.NONE;
                    break;
                default:
                    break;
            }


            for (TransactionId transactionId : candidates) {
                ArrayList<TransactionId> list = mapLock.get(transactionId);
                if (!list.isEmpty()) {
                    list.remove(tid);
                }
            }

            cond.signalAll();
        }

        boolean keepLocking(TransactionId tid) {
            boolean cond1 = owners.contains(tid);
            return cond1;
        }

        boolean tryPrivateLock(TransactionId tid) {
            switch (enumLock){
                case EXCLUSIVE:
                    return !owners.contains(tid);
                case SHARE:
                    return (owners.size() != 1) || !owners.contains(tid);
                default:
                    break;
            }
            return false;
        }

        boolean getPrivateLock(TransactionId tid) {
            if (tryPrivateLock(tid)) {
                return true;
            }
            enumLock = LockManager.WhichLock.EXCLUSIVE;
            owners.add(tid);

            for (TransactionId transactionId : candidates) {
                ArrayList<TransactionId> a = mapLock.computeIfAbsent(transactionId, k -> new ArrayList<>());
                a.add(tid);
            }
            return false;
        }



        void wait(LockManager.WhichLock whichLock, TransactionId tid) {
            boolean cond1 = !candidates.contains(tid);
            if (cond1) {
                candidates.add(tid);
                ArrayList<TransactionId> depend = mapLock.computeIfAbsent(tid, k -> new ArrayList<>());
                depend.addAll(owners);
            }

            try {
                switch (whichLock){
                    case EXCLUSIVE:
                        while ((tryPrivateLock(tid))){
                            cond.await(1, TimeUnit.SECONDS);
                        }
                        break;
                    case SHARE:
                        while (tryGetSharedLock(tid)) {
                            cond.await(1, TimeUnit.SECONDS);
                        }
                        break;
                    default:
                        break;
                }
            }
            catch (InterruptedException ignored) {
            }
            finally {
                candidates.remove(tid);
            }
        }


    }
}
