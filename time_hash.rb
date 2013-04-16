require 'time'

class Time_hash
  attr_accessor :d, :t, :utctime

  def initialize(time)
    make_invalid
    set_time(time) unless time.nil?
  end

  def set_time(new_time)
    if new_time.is_a?(Time)
      @utctime = new_time.utc
      sync
    elsif new_time.is_a?(Date)
      @utctime = new_time.to_time.utc
      sync
    elsif new_time.is_a?(Hash)
      begin
        str = "#{new_time['d']} #{new_time['t']}"
        @utctime = Time.parse(str).utc
        sync
      rescue
        make_invalid
        raise ArgumentError.new('Unable to parse hash value: expect {"d" => "2001-12-29","t" => "15:30:12"} ')
      end
    elsif new_time.is_a?(String)
      begin
        @utctime = Time.parse(new_time).utc
        sync
      rescue          
        make_invalid
        raise ArgumentError.new("Unable to parse string value")
      end
    else        
      make_invalid
      raise ArgumentError.new("Only accepts Time, Date, DateTime, String values")
    end
  end

  def sync
    @d = @utctime.strftime('%Y-%m-%d')
    @t = @utctime.strftime('%H:%M:%S')
    @valid = true
  end

  def is_valid?
    return @valid
  end

  def make_invalid
    @utctime =nil
    @d = nil
    @t = nil
    @valid = false
  end

  def to_hash
    if is_valid? 
      return {"d" => @d, "t" => @t} 
    else
      return {}
    end
  end
end

# th = Time_hash.new
# th.set_time('2013-04-15 13:28:36 UTC')
# th.d