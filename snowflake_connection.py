import os
import sys
import json
import base64
import getpass
import logging
import snowflake.connector
from optparse import OptionParser
from logging.handlers import RotatingFileHandler

class snowflake_connection:
    def __init__(self, verboseFlag=False):
        self.cnx = None
        self.verbose = False


    # def setupLogging(self):
    #     """ Sets up the rotating file logger for the parser. This records logs to a
    #     specified backup location, in this case ./logs/parser.log. The log file location
    #     is stored in a JSON configuration script passed to the CreateFeatures at runtime.
    #     """
    #     self.logger = logging.getLogger(__name__)
    #     handler = RotatingFileHandler('log/incremental_table_generator.log', maxBytes=5000000, backupCount=3)
    #     format = "%(asctime)s %(levelname)-8s %(message)s"
    #     handler.setFormatter(logging.Formatter(format))
    #     handler.setLevel(logging.INFO)
    #     self.logger.addHandler(handler)
    #     self.logger.setLevel(logging.INFO)


    def initConnection(self):
        username = input('Username:')
        password = getpass.getpass()
        accountName = 'hubspot.us-east-1'
        #accountFlag = input('Use default account name ({0}), [Y/N]:'.format(accountName))

        # while accountFlag.strip().upper() not in ['Y', 'N']:
        #     accountFlag = input('Use default account name ({0}), [Y/N]:'.format(accountName))
        #
        # # Use the default accountName unless indicated
        # if accountFlag.strip().upper() == 'N':
        #     accountName = input('Enter New Account Name:')
        #

        # Try to instantiate the connection to the SnowFlake cluster
        try:
            if self.verbose:
                print('Connecting...\n')
            self.ctx = snowflake.connector.connect(
                user=username,
                password=password,
                account=accountName,
                authenticator='https://hubspot.okta.com',
            )
            if self.verbose:
                print('Connection success.\n')
        except Exception as err:
            print('{0}'.format(err))
            print('Error - cannot connect to SnowFlake, exiting...')
            #self.logger.error('GDPR_Crawler:initConnection() - {0}'.format(err))
            return None

    def execute_query_with_result(self, sql_query):
        self.initConnection()

        try:
            if self.ctx is not None:
                cs = self.ctx.cursor()
                print("query to execute ", sql_query)
                sql_result = cs.execute(sql_query).fetchall()
                print(sql_result)
            else:
                print('Null connection to DB - {0}'.format(error))

        except Exception as error:
            print('Error running query'.format(error))
            cs.close()
            exit(1)

        finally:
            cs.close()

def main(argv):
    sf = snowflake_connection()
    sf.execute_query_with_result(sql_query)


if __name__ == "__main__":
    sf = snowflake_connection()
    sql_query = sys.argv[1]
    sys.exit(sf.execute_query_with_result(sql_query))
    #sys.exit(main(sys.argv))




#my_s = snowflake_connection()
#my_s.execute_query_with_result("select * from prod_db.fact_tables.partner_type_events where partneraccountid = '6675'")
