package cn.edu.tsinghua.iginx.mongodb.immigrant.tools;

import cn.edu.tsinghua.iginx.mongodb.immigrant.entity.Query;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import com.mongodb.client.model.Updates;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.Document;
import org.bson.conversions.Bson;

public class UpdateUtils {
  public static Bson unset(List<String> paths) {
    assert !paths.isEmpty();

    List<Bson> unsetList =
        paths.stream().map(NameUtils::encodePath).map(Updates::unset).collect(Collectors.toList());
    return Updates.combine(unsetList);
  }

  public static List<Bson> wildcardUnset(List<String> pathPatterns) {
    assert !pathPatterns.isEmpty();

    // {
    //   $replaceWith: {
    //      $mergeObjects:[
    //         {"_id": "$_id"},
    //         {
    //            $arrayToObject:{
    //               $filter: {
    //                  input: {
    //                     $objectToArray: "$$ROOT"
    //                  },
    //                  cond: {
    //                     $not:[
    //                        {
    //                           $or:[
    //                              {
    //                                 $regexMatch: {
    //                                    input: "$$this.k",
    //                                    regex: <pathPatterns[0]>
    //                                 }
    //                              }
    //                           ]
    //                        }
    //                     ]
    //                  }
    //               }
    //            }
    //         }
    //      ]
    //   }
    // }
    List<Bson> fieldRegexList =
        pathPatterns.stream()
            .map(NameUtils::encodePath)
            .map(pattern -> Query.Utils.regexMatch("$$this.k", pattern))
            .collect(Collectors.toList());

    Bson orRegexList = new Document("$or", fieldRegexList);
    Bson cond = new Document("$not", Collections.singletonList(orRegexList));
    return Collections.singletonList(Query.Utils.filterRoot(cond));
  }
}
