import ranking
from ranking.rule_based_models import MostLikedModel

def get_model_from_config_spec(model_arch):
    if model_arch == 'MostLikedModel':
        model = MostLikedModel()
    return model


