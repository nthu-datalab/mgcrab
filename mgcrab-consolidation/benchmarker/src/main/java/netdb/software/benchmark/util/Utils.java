package netdb.software.benchmark.util;

public class Utils {

	public static int[] randomNoRepeat(int from, int to, int count) {
		int size = to - from + 1;
		int[] array = new int[size];
		int[] answer = new int[count];
		for (int i = 0; i < array.length; i++)
			array[i] = from + i;

		for (int i = 0; i < count; i++) {
			int rand = (int) (Math.random() * (size - i));
			int tmp = array[i];
			array[i] = array[rand + i];
			array[rand + i] = tmp;
			answer[i] = array[i];

		}
		return answer;
	}

	public static void shuffleArray(Object[] array) {
		int arraySize = array.length;
		for (int i = 0; i < arraySize; i++) {
			int rand = (int) (Math.random() * (arraySize - i));
			Object tmp = array[i];
			array[i] = array[rand + i];
			array[rand + i] = tmp;
		}
	}
}
