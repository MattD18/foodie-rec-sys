from ranking.base_model import RankingModel

class MostLikedModel(RankingModel):
    
    def __init__(self):
        self.most_like_restaurant_id_list = None
    
    def fit(self, df):
        self.most_like_restaurant_id_list = \
            df.loc[df['label']==1].groupby('restaurant_id').apply(lambda x: x['label'].sum()).index.tolist()
        
    def predict(self):
        return self.most_like_restaurant_id_list
        