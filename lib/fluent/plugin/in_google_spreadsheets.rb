require "rubygems"
require "google_drive"

module Fluent

  class GoogleSpreadSheetInput < Input
    Plugin.register_input('google_spreadsheet', self)

    config_param :tag,            :string,  :default => nil
    config_param :client_id,      :string,  :default => nil
    config_param :client_secret,  :string,  :default => nil
    config_param :refresh_token,  :string,  :default => nil
    config_param :sheet_id,       :string,  :default => nil
    config_param :run_interval,   :time,    :default => 10

    def initialize
      super
    end

    def configure(conf)
      super
      #@requests = []
      #conf.elements.each do | element |
      #  touch_recursive(element)
      #
      #  if element.name == "request" then
      #    @requests.push(element)
      #  end
      #end
    end

    def start
      super
      @finishd = false
      @client = OAuth2::Client.new(
          @client_id,
          @client_secret,
          site: "https://accounts.google.com",
          token_url: "/o/oauth2/token",
          authorize_url: "/o/oauth2/auth")
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @finishd = true
      @thread.join
    end

    def run
      until @finishd
        sleep @run_interval
        responses = []

        begin
          get_val()
        rescue => e
          log.warn "Failed: #{e.message}"
          log.debug e
        end
      end
    end

    def get_val()

      auth_token = OAuth2::AccessToken.from_hash(@client, {:refresh_token => @refresh_token, :expires_at => 3600})
      auth_token = auth_token.refresh!
      session = GoogleDrive.login_with_oauth(auth_token.token)
      ws = session.spreadsheet_by_key(@sheet_id).worksheets[0]

      for row in 2..ws.num_rows
          ret = Hash.new()
          for col in 1..ws.num_cols
            ret[ws[1, col]] = ws[row, col]
          end
          Engine.emit(@tag, Engine.now.to_i, ret)
      end
    end
  end
end
