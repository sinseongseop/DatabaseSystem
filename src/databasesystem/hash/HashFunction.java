package databasesystem.hash;

public class HashFunction {

    // partition을 만들 떄 사용하는 해시 함수
    public static int partitionHash(String input, int range) {
        int hashValue = 0;
        for (char charValue : input.toCharArray()) {
            hashValue += charValue;
        }
        return Math.abs(hashValue % range);
    }

    // 해시 인덱스를 구축하는 데 사용할 해시 함수
    public static int indexHash(String input, int range) {
        int hashValue = input.hashCode();
        return Math.abs(hashValue % range);
    }

}
