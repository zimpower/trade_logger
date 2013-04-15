#
#  Representation of a trade and a couple of helper functions
require 'yaml'   
require 'json'

# Helper function for writing yaml and json - extending the Hash class
# open the Hash class and add the deep_stringify_keys
class Hash
  def deep_stringify_keys
    new_hash = {}
    self.each do |key, value|
      new_hash.merge!(key.to_s => (value.is_a?(Hash) ? value.deep_stringify_keys : value))
    end
  end
end



class Trade
  attr_accessor :data

  def initialize(trade = {})
    @data = trade
  end

  def id
    return @data[:id]
  end

  # Pretty Yaml file writing
  def append(filename, type = :yaml)
    if @data 
      File.open(filename, "a") do |f|
        if type == :yaml 
          f.write(Trade::yaml(@data)) 
        elsif type == :json
          f.write(Trade::json(@data)) 
        else
          # unknown file-type
        end
      end
    end
  end

  # Helper Class function to create pretty yaml
  def self.yaml(hash)
    string = hash.deep_stringify_keys.to_yaml
    #  string.gsub("!ruby/symbol ", ":").sub("---","").split("\n").map(&:rstrip).join("\n").strip
  end

  # Helper Class function to create pretty json
  def self.json(hash)
    string = hash.deep_stringify_keys.to_json
    #  string.gsub("!ruby/symbol ", ":").sub("---","").split("\n").map(&:rstrip).join("\n").strip
  end
  
  
end
