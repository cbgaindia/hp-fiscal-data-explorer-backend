from datetime import datetime
import falcon

class CORSMiddleware:
    '''
    Middleware for handling CORS.
    '''
    def process_request(self, req, resp):
        '''
        process request to set headers.
        '''
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.set_header('Access-Control-Allow-Headers', '*')
        resp.set_header('Access-Control-Allow-Methods', '*')

        
def validate_date(req, resp, resource, params):
    '''
    check for required parameters in query string and validate date format.
    '''
    params = req.params
    if 'start' not in params or 'end' not in params:
        resp.status = falcon.HTTP_400  #pylint: disable=no-member
        raise falcon.HTTPBadRequest('Incomplete Request', 'start and end date is required')  #pylint: disable=no-member
    try:
        datetime.strptime(params['start'], '%Y-%m-%d')
        datetime.strptime(params['end'], '%Y-%m-%d')
    except ValueError as err:
        resp.status = falcon.HTTP_400  #pylint: disable=no-member
        raise falcon.HTTPBadRequest('Invalid Params', str(err))  #pylint: disable=no-member

def validate_vis_range(req, resp, resource, params):
    '''
    check for required parameters in query string and validate date format.
    '''
    params = req.params
    vis_range = ['Week','Month']
    if 'range' not in params or params['range'] not in vis_range:
        resp.status = falcon.HTTP_400  #pylint: disable=no-member
        raise falcon.HTTPBadRequest('Incomplete Request', 'please specify correct vis range (WEEK, MONTH)')  #pylint: disable=no-member
