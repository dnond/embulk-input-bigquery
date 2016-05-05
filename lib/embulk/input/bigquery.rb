require 'bigquery'

module Embulk
  module Input
    class Bigquery < InputPlugin
      Plugin.register_input("bigquery", self)

      class << self
        def transaction(config, &control)
          params = {
            project:          config.param('project_id', :string),
            dataset:          config.param('dataset_id', :string),
            table:            config.param('table_id', :string),
            email:            config.param('service_email', :string),
            private_key_path: config.param('service_account_file', :string),
            threads:          1
          }

          @client = BQ.client(params)
          schema = @client.fetch_schema(params[:table])
          table_schema = format_schema(schema)

          yield(
            {
              'table'        => "#{params[:dataset]}.#{params[:table]}",
              'table_schema' => table_schema
            },
            columns(table_schema),
            params[:threads]
          )

          {}
        end

        def columns(table_schema)
          table_schema.map.with_index do |(key, type), index|
            column_type = case type
                          when 'INTEGER' then
                            :long
                          when 'STRING' then
                            :string
                           when 'TIMESTAMP' then
                            :timestamp
                          end
            Column.new(index, key, column_type)
          end
        end

        def format_schema(schema)
          schema.each_with_object({}) { |s, hash| hash[s['name']] = s['type'] }
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
        columns = @table_schema.keys.join(',')
        client.sql("SELECT #{columns} FROM #{@table}")
      end

      def client
        self.class.instance_variable_get(:@client)
      end

      def values_in(row)
        row.map do |key, value|
          case @table_schema[key]
          when 'INTEGER' then
            value.to_i
          else
            value
          end
        end
      end
    end
  end
end
