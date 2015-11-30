import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Created by Fuga on 15/11/25.
 */
public class RandomMatrix {
    public static void main(String[] args) throws IOException {
        int rowA = 3000;
        int colA = 2000;
        int colB = 2500;

        Random random = new Random();
        BufferedWriter bw = new BufferedWriter(new FileWriter("m1.csv"));
        for (int i = 1;i<=rowA;i++) {
            for (int j = 1;j<=colA;j++) {
                int element = random.nextInt()%10;
                bw.write(i + "," + j + "," + element);
                bw.newLine();
            }
        }
        bw.close();
        bw = new BufferedWriter(new FileWriter("m2.csv"));
        for (int i = 1;i<=colA;i++) {
            for (int j = 1;j<=colB;j++) {
                int element = random.nextInt()%10;
                bw.write(i + "," + j + "," + element);
                bw.newLine();
            }
        }
        bw.close();
    }
}
