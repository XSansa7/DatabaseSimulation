public class LockInfo {
  private int transactionNum;
  private int lockType;

  public LockInfo(int trans, int lock) {
  	transactionNum = trans;
	lockType = lock;
  }

  public int getTransactionNum() {
  	return transactionNum;
  }

  public int getLockType() {
  	return lockType;
  }

  public void updateLockType(int l){
  	lockType=l;
  }
}