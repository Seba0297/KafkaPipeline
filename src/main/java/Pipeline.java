public class Pipeline {
    public static void main(String[] args) throws InterruptedException {

        new StageA("groupStageA", "ID1-StageA", 1);
        System.out.println("StageA is on.");
        new StageB1("groupStageB1", "ID2a-StageB1", 1);
        System.out.println("StageB1 is on.");
        new StageB2("groupStageB2", "ID2b-StageB2", 1);
        System.out.println("StageB2 is on.");
        new StageC("groupStageC", "ID3-StageC", 1);
        System.out.println("StageC is on.");

    }
}
