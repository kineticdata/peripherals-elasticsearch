# Require the dependencies file to load the vendor libraries
require File.expand_path(File.join(File.dirname(__FILE__), "dependencies"))

class ElasticsearchDocumentCreateV1
  # Prepare for execution by building Hash objects for necessary values and
  # validating the present state.  This method sets the following instance
  # variables:
  # * @input_document - A REXML::Document object that represents the input Xml.
  # * @info_values - A Hash of info names to info values.
  # * @parameters - A Hash of parameter names to parameter values.
  #
  # This is a required method that is automatically called by the Kinetic Task
  # Engine.
  #
  # ==== Parameters
  # * +input+ - The String of Xml that was built by evaluating the node.xml
  #   handler template.
  def initialize(input)
    # Set the input document attribute
    @input_document = REXML::Document.new(input)
    
    # Retrieve all of the handler info values and store them in a hash variable named @info_values.
    @info_values = {}
    REXML::XPath.each(@input_document, "/handler/infos/info") do |item|
      @info_values[item.attributes["name"]] = item.text.to_s.strip
    end

    # Retrieve all of the handler parameters and store them in a hash variable named @parameters.
    @parameters = {}
    REXML::XPath.each(@input_document, "/handler/parameters/parameter") do |item|
      @parameters[item.attributes["name"]] = item.text.to_s.strip
    end
  end

  # The execute method gets called by the task engine when the handler's node is processed. It is
  # responsible for performing whatever action the name indicates.
  # If it returns a result, it will be in a special XML format that the task engine expects. These
  # results will then be available to subsequent tasks in the process.
  def execute
    api_username    = URI.encode(@info_values["api_username"])
    api_password    = @info_values["api_password"]
    api_server      = @info_values["api_server"]
    index           = @parameters["index"]
    type            = @parameters["type"]
    id              = @parameters["id"]
    version         = @parameters["version"]
    pipeline_id     = @parameters["pipeline_id"]
    document_json   = @parameters["document_json"]
    error_handling  = @parameters["error_handling"]

    api_route = "#{api_server.chomp("/")}/#{index}/"
    api_route. << "#{URI.encode(type)}/" if type.to_s.strip.empty? == false
    api_route << "#{URI.encode(id)}/" if id.to_s.strip.empty? == false
    api_route << "_create"
    if pipeline_id.to_s.strip.empty? == false || version.to_s.strip.empty? == false then
      api_route << "?"
    end
    api_route << "pipeline=#{URI.encode(pipeline_id)}" if pipeline_id.to_s.strip.empty? == false
    api_route << "#{'&' if pipeline_id.to_s.strip.empty? == false}version=#{URI.encode(version)}" if version.to_s.strip.empty? == false
    
    puts "API ROUTE: #{api_route}"

    if api_username.to_s.strip.empty? == false && api_password.to_s.empty? == false then
      resource = RestClient::Resource.new(api_route, { :user => api_username, :password => api_password })
    else
      resource = RestClient::Resource.new(api_route)
    end

    response = resource.put(document_json, { accept: :json, content_type: :json })

    # Build the results to be returned by this handler
    <<-RESULTS
    <results>
      <result name="Handler Error Message"></result>
    </results>
    RESULTS

    rescue RestClient::Exception => error
      error_message = JSON.parse(error.response)["error"]
      if error_handling == "Raise Error"
        raise error_message
      else
        <<-RESULTS
        <results>
          <result name="Handler Error Message">#{error.http_code}: #{escape(error_message)}</result>
        </results>
        RESULTS
      end
  end


  ##############################################################################
  # General handler utility functions
  ##############################################################################

  # This is a template method that is used to escape results values (returned in
  # execute) that would cause the XML to be invalid.  This method is not
  # necessary if values do not contain character that have special meaning in
  # XML (&, ", <, and >), however it is a good practice to use it for all return
  # variable results in case the value could include one of those characters in
  # the future.  This method can be copied and reused between handlers.
  def escape(string)
    # Globally replace characters based on the ESCAPE_CHARACTERS constant
    string.to_s.gsub(/[&"><]/) { |special| ESCAPE_CHARACTERS[special] } if string
  end
  # This is a ruby constant that is used by the escape method
  ESCAPE_CHARACTERS = {'&'=>'&amp;', '>'=>'&gt;', '<'=>'&lt;', '"' => '&quot;'}
end
