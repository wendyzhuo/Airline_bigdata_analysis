/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package final_mahout;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 *
 * @author Zhuang Zhuo <zhuo.z@husky.neu.edu>
 */
public class Final_Mahout {

    /**
     * @param args the command line arguments
     */
   public static void main(String[] args) throws TasteException, IOException {
        DataModel model= new FileDataModel(new File("/Users/wendyzhuo/Desktop/data3.csv"));
        //Computer the similarity between users,according to their preference
        UserSimilarity similarity=new EuclideanDistanceSimilarity(model);
    
        //Group the users with similar preference
        UserNeighborhood neighborhood= new ThresholdUserNeighborhood(0.2,similarity,model);
    
        //Create a recommender
        UserBasedRecommender recommender=new GenericUserBasedRecommender(model,neighborhood,similarity);
        //For the user with the id 1 get two recommendations
       
        List<RecommendedItem>recommendations= recommender.recommend(1, 2);
        for(RecommendedItem recommendation : recommendations){
            System.out.println("they should not take id: "
                    +recommendation.getItemID()+"(predicted preference:"
                    +recommendation.getValue()+")");
        }
        
        // TODO code application logic here
    }
    
}
    

