require 'time'
require 'date'
class MysqlQueryWithRetry < Scout::Plugin
  needs 'mysql'

  OPTIONS=<<-EOS
  host:
    name: Host
    notes: The host to query
    default: 127.0.0.1
  port:
    name: Port
    notes: The port number on the slave host
    default: 3306
  username:
    name: Username
    notes: The MySQL username to use
    default: root
  password:
    name: Password
    notes: The password for the mysql user
    default:
    attributes: password
  query:
    name: Query
    notes: Query to run - output should be 1 number or rows of (label, number)
    default: select 'dummy', 0 from dual
  retry:
    name: Retry
    notes: Amount of times to retry before failing
    default: 1
  retryalert:
    name: RetryAlert
    notes: Alert on error of all retries (true or false)
    default: true
  EOS

  attr_accessor :connection

  def build_report
    count = option(:retry).to_i
    counter = 0
    reported = 0
    count.times do
      begin
        counter = counter + 1
        self.connection=Mysql.new(option(:host),option(:username),option(:password),nil,option(:port).to_i)
        # Should sanitize query somehow
        h=connection.query(option(:query))
        if h.nil?
          error("No data returned from query")
        else
          h.each do |row|
            if row.length < 2
              report("value"=>row[0])
              reported = 1
              break # Only 1 unnamed value allowed
            end
            if not row[0].nil? and row[0].strip.length > 0
              report(row[0]=>row[1])
              reported = 1
            else
              report("(none)"=>row[1])
              reported = 1
            end
          end
        end
        
      rescue Mysql::Error=>e
        if ( ( count - counter ) <= 1 )
          error("Aborting - Unable to connect to MySQL",e.to_s)
        else
          if (option(:retryalert))
            alert("Retrying - Unable to connect to MySQL",e.to_s)
          end
        end
      end
      if(reported == 1)
        break
      end
    end
  end
end
