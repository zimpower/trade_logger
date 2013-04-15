require "webrick"
require "mongo"
require "logger"

HOST = "centenary.local"
DB = "dtcc_trades"
COLLECTION = "trade_list"

$LOG = Logger.new('logs/trade_webserver.log', 'daily')
HEADERS = %w[id title time_stamp und acc und_not acc_not expiry strike type prem_ccy prem]
DISPLAY_FIELDS = { :fields => HEADERS }
RESTRICT = 50


class Simple < WEBrick::HTTPServlet::AbstractServlet
  def initialize server
    super server
    init_db
  end

  def init_db
    @mgo_cleint= nil
    @trades = nil
    $LOG.info "Attempting connection to #{HOST}..."
    begin
      @mgo_client = Mongo::MongoClient.new(HOST) 
    rescue Mongo::ConnectionFailure => e
      $LOG.error "Attempting to connect to '#{HOST}' : #{exception_message(e)}"  
      raise e
    else
      $LOG.info "Success : connected to #{HOST}"
    end

    @trades = @mgo_client.db(DB).collection(COLLECTION)
  end



  def do_GET(request, response)

    response.status = 200
    response['Content-Type'] = "text/html"
    body = '<title>Trade Logger</title>'
    body += "<link rel='stylesheet' type='text/css' href='../../public_html/style.css'>"    
    body += "<h2> DTCC Trade logger web server </h2>"

    # Hande count/
    if request.path == "/count" || request.path == "/count/all"

      body += "<p>There are #{@trades.count()} trades captured </p>"

      # Hande id=
    elsif request.path =~ /^\/id=(\d+)/i

      body += "<h3> Id: '#{$1}' </h3>"        

      results = @trades.find( {:id => "#{$1}"},  DISPLAY_FIELDS)
      results.each { |res|  
        body += HashToHTML(res)
      } 

      # Hande id~ 
    elsif request.path =~ /^\/id~(\d+)/i
      body += "<h3> Id: like '#{$1}' </h3>"        

      re = %r{#{$1}}
      results = @trades.find( {:id => re} , DISPLAY_FIELDS )
      results.each { |res|  
        body += HashToHTML(res)
      } 

      # Handle datetimes with a to and from
    elsif request.path =~ /^\/dates\/(.+?)\/(.+)/i
      body += "<h3> Dates: from '#{$1}' to '#{$2}'</h3>"
      start_date = Time.parse($1).utc
      end_date = Time.parse($2).utc
      
      query = {:time_stamp => {:$gte => start_date, :$lte => end_date}}
      
      results = @trades.find( query, DISPLAY_FIELDS ).sort( :time_stamp => 1 ).limit(RESTRICT)
      body += "<p>From: <strong>#{start_date}</strong>   To: <strong>#{end_date}</strong><p>"
      body += "<p>Displaying first 50 of <strong>#{results.count}</strong> sorted on <code>timestamp</code><p>"

      results.each { |res|  
        body += HashToHTML(res)
      }

      # Handle datetimes with a to only

    elsif request.path =~ /^\/dates\/(.*)/i
      body += "<h3> Dates: after '#{$1}'</h3>"
      start_date = Time.parse($1).utc

      query = {:time_stamp => {:$gte => start_date } }

      results = @trades.find(query , DISPLAY_FIELDS ).sort( :time_stamp => 1 ).limit(RESTRICT)
      body += "<p>After: <strong>#{start_date}</strong><p>"
      body += "<p>Displaying first 50 of <strong>#{results.count}</strong> sorted on <code>timestamp</code><p>"

      body += array_to_HTML(results.to_a,HEADERS)
      # results.each { |res|  
      #   body += HashToHTML(res)
      # }
    end

    body += "<h3> Usage </h3><ul>"
    body += "<li><code>localhost:8000<strong>/count</strong></code> display a count of all records"
    body += "<li><code>localhost:8000<strong>/id=123456789</strong></code> find the records with id"
    body += "<li><code>localhost:8000<strong>/id~1234567</strong></code> show record with ids like id"
    body += "<li><code>localhost:8000<strong>/dates/10dec12</strong></code> show records after specfied date"
    body += "<li><code>localhost:8000<strong>/dates/10dec12/20dec12</strong></code> show records between dates"
    body += "<li><code>localhost:8000<strong>/dates/10dec12T03:00/20dec12T15:30</strong></code> show records between dates and times"
    body += "</ul>"

    response.body = body
  end

  def array_to_HTML(rows, header)
    
    headers = "<tr>#{to_cells(header, 'th')}</tr>"

    cells = rows.map do |row| 
      "<tr>#{to_cells(row.values_at(*header),'td')}</tr>"
    end.join("\n  ")

    table = "<table id ='hor-minimalist-b'>
      <thead> #{headers} </thead>
      <tbody> #{cells} </tbody>
    </table>"
    return table
  end

  # Prints nested Hash as a nested <ul> and <li> tags
  # - keys are wrapped in <strong> tags
  # - values are wrapped in <span> tags
  def HashToHTML(hash, indent = 2)
    return if !hash.is_a?(Hash)

    indent_level = indent

    out = " " * indent_level + "<ul>\n"

    hash.each do |key, value|
      out += " " * (indent_level + 2) + "<li><strong>#{key}:</strong>"

      if value.is_a?(Hash)
        out += "\n" + HashToHTML(value, :indent_level => indent_level + 2) + " " * (indent_level + 2) + "</li>\n"
      else
        out += " <span>#{value}</span></li>\n"
      end
    end

    out += " " * indent_level + "</ul>\n"
  end

    def to_cells(array, tag)
      array.map { |c| "<#{tag}>#{c}</#{tag}>" }.join
    end

end





if $0 == __FILE__ then

  # log in the mongo db

  html_root = File.dirname(__FILE__)
  server = WEBrick::HTTPServer.new(:Port => 8000, :DocumentRoot => html_root)

  server.mount "/get/", Simple

  trap 'INT' do 
    server.shutdown 
  end
  server.start
end
