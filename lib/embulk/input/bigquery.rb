require 'pry'
require 'bigquery'

module Embulk
  module Input
    class Bigquery < InputPlugin
      Plugin.register_input("bigquery", self)

      def self.transaction(config, &control)
        params = {
          project:          config.param('project_id', :string),
          dataset:          config.param('dataset_id', :string),
          table:            config.param('table_id', :string),
          email:            config.param('service_email', :string),
          private_key_path: config.param('service_account_file', :string),
          threads:          1
        }

        @client = BQ.client(params)
        table_schema = @client.fetch_schema(params[:table])

        task = {
                 'table'        => "#{params[:dataset]}.#{params[:table]}",
                 'table_schema' => table_schema,
               }
        yield(task, columns(table_schema), params[:threads])

        {}
      end

      def self.columns(table_schema)
        table_schema.map.with_index do |schema, index|
          column_type = case schema['type']
                        when 'INTEGER' then
                          :long
                        when 'STRING' then
                          :string
                        end
          Column.new(index, schema['name'], column_type)
        end
      end

      def init
        @table_schema = task['table_schema']
        @table = task['table']
      end

      def run
        rows_in_table.each do |row|
          values = values_in row
          page_builder.add values
        end
        page_builder.finish

        task_report = {}
        return task_report
      end

      private

      def rows_in_table
        client.sql("SELECT * FROM #{@table}")
      end

      def client
        self.class.instance_variable_get(:@client)
      end

      def values_in(row)
        row.map do |key, value|
          case types_in_schema[key]
          when 'INTEGER' then
            value.to_i
          else
            value
          end
        end
      end

      def types_in_schema
        @types_in_schema ||= Hash[*@table_schema.map { |s| [s['name'], s['type']] }.flatten]
      end
    end
  end
end
