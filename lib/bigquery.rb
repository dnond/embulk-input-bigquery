require 'bigquery-client'

module BQ
  class << self
    def client(params)
      @client unless @client.nil?
      params[:private_key_passphrase] ||= 'notasecret'
      params[:auth_method]            ||= 'private_key'

      @client = BigQuery::Client.new(params)
    end
  end
end
