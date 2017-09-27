public class Driver {
    /*

    1.1 pagerank的假设

　　数量假设：每个网页都会给它的链接网页投票，假设这个网页有n个链接，则该网页给每个链接平分投1/n票。

　　质量假设：一个网页的pagerank值越大，则它的投票越重要。表现为将它的pagerank值作为它投票的加权值。

     */
    public static void main(String args[]) throws  Exception{
        UnitMutiplication mutiplication = new UnitMutiplication();
        UnitClass sum = new UnitClass();

        String transitionMatrix = args[0]; //transition.txt directory
        String prMatrix = args[1];
        String subPageRank = args[2]; //dir sub pr
        int count = Integer.parseInt(args[3]); //迭代的次数

        for (int i = 0; i < count; i++) {
            // transitionMatrix doesn't change
            String [] args1 = {transitionMatrix, prMatrix + i, subPageRank + i};
            mutiplication.main(args1);
            String [] args2 = {subPageRank + i, prMatrix + (i + 1)};
            sum.main(args2);
        }
    }
}
