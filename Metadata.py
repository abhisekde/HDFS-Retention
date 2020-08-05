from datetime import datetime
class Metadata:
    '''
    Dataset metadata
    Properties: _name, _pond, _last_used, _path
    '''
    def __init__(self, _name, _pond, _last_used, _path):
        '''
        _name, _pond, _last_used, _path
        '''
        self._name = _name
        self._storage = _pond
        self._last_used = _last_used
        self._age_days = (datetime.now()-_last_used).days
        self._path = _path
        
    def to_dict(self):
        return {'name': self._name, 
                'pond': self._storage, 
                'last_used': self._last_used, 
                'age_days': self._age_days,
                'path': self._path
               }
    
    def __str__(self):
        return self.to_dict().__str__()


