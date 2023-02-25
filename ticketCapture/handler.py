from ladon.server.wsgi import LadonWSGIApplication
from os.path import abspath,dirname

application = LadonWSGIApplication(
  ['capture'],
  [dirname(abspath(__file__))],
  catalog_name='My Ladon webservice catalog',
  catalog_desc='This is the root of TRAACS WAVE Ticket Capture')

