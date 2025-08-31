Supporting OData V2 and V4 Read-Only JSON in DuckDB Service

1. Protocol and Data Model Differences (OData V2 vs OData V4)

Versioning and Standards: OData V2 was a Microsoft-specific protocol (released under an Open Specification Promise), whereas OData V4 is an OASIS/ISO standard with significant changes ￼. V4 was designed to be quite different from V2 in order to simplify and extend the protocol. One key change is in the supported formats: OData 2.0 requires supporting both XML (Atom) and JSON for data payloads, while OData 4.0 makes JSON the primary format (XML is optional for data) ￼. For a read-only service, this means a V2 implementation should technically support Atom/XML by spec, but since the goal is JSON-only, you would intentionally deviate from the V2 default (by spec, a request with no Accept header defaults to Atom in V2 ￼). In contrast, a V4 service is typically JSON-only for data by design (though V4 still uses XML for metadata by default) ￼.

Data Model and EDM Types: The underlying Entity Data Model (EDM) in V4 includes new primitive types and constructs not present in V2. For example, OData V4 adds types like Edm.Date (date without time), Edm.TimeOfDay, Edm.Duration, and enumeration types, which have no direct equivalents in V2 ￼. In OData V2, date/time values had to be represented as Edm.DateTime or Edm.DateTimeOffset (usually with XML dateTime or ticks format) and Edm.Time (a time span) – these were later refined in V4 into separate date and time types. If your DuckDB schema uses dates or times, you’ll need to map those appropriately: e.g. a pure date in DuckDB might be exposed as Edm.Date in V4 but as an Edm.DateTime (with a time convention of midnight) in a V2 $metadata. Similarly, V4’s single-precision floating type is called Edm.Single, whereas older documentation often referred to it as “Edm.Float” in V2 contexts ￼. The upshot is that your V2 and V4 metadata will differ in the EDM type names for certain columns (e.g. a DuckDB DATE becomes Edm.Date in V4 metadata vs Edm.DateTime in V2; a BOOL remains Edm.Boolean in both, etc.). Ensure your implementation can present the correct types per version and handle value serialization accordingly (for instance, Edm.DateTime in V2 JSON is typically serialized as a special “/Date( ticks )/” format ￼ ￼, whereas Edm.Date in V4 JSON would serialize as an ISO date string like "YYYY-MM-DD" in the JSON payload).

Entity Model and Relationships: OData V4 streamlined the way relationships are represented in the metadata and payload. In OData V2, associations between entities are declared as separate <Association> and <AssociationSet> elements in the CSDL, and navigation properties on entities refer to those association definitions. In OData V4, the association concept is integrated directly into the navigation property declarations – each <NavigationProperty> on an entity type directly defines its partner and referential constraints (no separate <Association> element) ￼ ￼. For a read-only service, this mostly affects how you generate the $metadata (discussed later), but it also subtly affects certain URLs and operations. For example, in V2 the URL to manipulate or read links was .../Products(1)/$links/Category (returning an XML fragment with a <uri>), whereas in V4 the equivalent would be .../Products(1)/Category/$ref (returning a JSON with an @odata.id or a 204 status) – a different URI convention and representation. If your focus is strictly data retrieval of entities (and you don’t plan to implement separate link retrieval endpoints), you might not need to implement the $links or $ref routes, but be aware of the difference if the need arises.

Functions, Actions and Other Capabilities: OData V4 introduced actions (server-side operations that may modify data) and made function imports more powerful (including bound functions). OData V2 supports only function imports (no actions) and those are invoked via specific GET or POST requests. Since we are focusing on read-only, you likely won’t implement actions, but you might have function imports that read data (e.g. a function that returns a computed result). Those can be supported in V2 as “Service Operations” invoked via GET (for reads) ￼. In V4, similar functionality would be an unbound function (still invoked via GET with .../FunctionName(param=...)). Just ensure any existing function imports in your model are exposed properly in both versions’ metadata and that the invocation URL conventions match the version (they are largely similar in basic usage, aside from JSON format of the result which will follow the respective payload conventions).

Other Notable Differences: OData V4 supports some features not present in V2 that affect query semantics: for example, $batch requests (batching multiple operations in one HTTP call) have a different format in V4 (JSON-based format with MIME multipart) whereas V2’s batching was based purely on MIME multipart. For a read-only service you might not need batch support; but if you do, note that V2 batch is slightly different (older Atom-based changesets and boundaries). Another difference is $search, a full-text search option introduced in V4 – not applicable in V2 (any search capability would be a custom query option in V2, if provided at all) ￼. Also, OData V4 allows singletons (single-instance entity exposed without a key, e.g. Me/ or a current user) and contained entities (entities without an entity set, contained in a parent) – these concepts don’t exist in V2. If your service is simple (exposing DuckDB tables as entity sets), you likely won’t use those advanced concepts, but it’s good to know that V2 clients cannot address singletons or contained nav properties; those must be exposed differently or not at all to V2 clients.

In summary, at the protocol level OData V4 is more modern and feature-rich, but for read-only data retrieval, V2 and V4 share the same basic principles (entities, collections, navigation links, filtering, etc.). The main differences that will surface in a JSON-only read service are in the format of responses and metadata, the available system query options, and some conventions in URIs, which we’ll discuss in detail below.

2. JSON Payload Format Differences (V2 vs V4)

When supporting JSON payloads for both OData V2 and V4, you must handle two distinct JSON formats. OData V2’s JSON format is often called “verbose JSON” due to its extra wrapping and metadata, whereas OData V4 uses the newer streamlined JSON format (often with minimal metadata). The table below highlights the key differences in JSON response structure:

Aspect	OData V2 JSON (Verbose)	OData V4 JSON (Minimal Metadata)
Top-Level Wrapper	Entire payload is wrapped in a top-level object with a single property "d", which contains the result. ￼ This wrapper is always present in responses (to ensure the JSON is not directly executable JavaScript).	No "d" wrapper. The payload is a JSON object that may include OData control annotations (properties starting with @odata.) and the result data directly. For example, the top-level may contain "@odata.context" and "value" properties instead of a nested "d".
Collections (Feeds)	A collection of entities is represented as {"d": { "results": [ {...}, {...} ] } }. In OData V1, they used d: [ {...} ], but V2 introduced the "results" array inside d to allow additional feed-level metadata ￼ ￼. If server-side paging or inline count is used, the d object can also include "__count" and "__next" fields (note the double underscore prefix) alongside "results" ￼ ￼.	A collection is represented as a JSON object with a "value" property holding an array of entities. Example: { "@odata.context": "...$metadata#Products", "value": [ { ... }, { ... } ], "@odata.nextLink": "url" }. The count (if requested via $count=true) is provided as an "@odata.count" property (an integer) on the top-level object ￼ ￼. Next-page links use "@odata.nextLink" (no underscores or inside “d”).
Single Entity (Entry)	A single entity is {"d": { /* entity properties... */ } }. Inside the d object, an "__metadata" field provides metadata for that entry ￼ ￼. All actual properties of the entity are sibling to "__metadata". Example: {"d": {"__metadata": {"uri": "...", "type": "Namespace.EntityType"}, "ID": 0, "Name": "Foo", ... } }.	A single entity is returned as a JSON object with its properties and any needed annotations. There is no separate metadata object per entry in minimal metadata mode. Instead, context and metadata are conveyed via annotations like "@odata.context" at the top and possibly per-property or per-entry annotations if needed. In minimal mode, type information is usually omitted unless the entry is of a derived type. For instance: { "@odata.context": "...$metadata#Products/$entity", "ID": 0, "Name": "Foo", ... } ￼. (In full metadata mode, V4 would include annotations like "@odata.id", "@odata.etag", "@odata.editLink", and "@odata.type" as needed ￼, but minimal keeps things lean.)
Metadata per Entry	Each entity in V2 JSON carries a __metadata object. This contains at least the uri (canonical URL) and type of the entity, and possibly an etag if concurrency is supported ￼ ￼. It also may include media link info if the entity is a media entry (stream). The type is sometimes omitted if the entity is of the expected base type (only included if it’s a derived subtype) ￼.	In V4 JSON minimal format, no verbose metadata object per entry. Instead, type and ID are usually inferred from context. If an entry is of a derived type, an "@odata.type" annotation might appear in the entry to specify its type. If the entity is a media entity or has an ETag, you might see "@odata.mediaReadLink", "@odata.editLink", "@odata.etag", etc., but these appear as needed as individual annotations on the entry object, not as a nested metadata object. In full metadata mode, each entry would include "@odata.id", "@odata.etag", "@odata.editLink", "@odata.type", etc. ￼. Minimal mode omits most of these to reduce payload size ￼.
Property Values	Primitive properties are represented as JSON values (number, string, boolean, etc.) with two notable exceptions: 64-bit integers and decimals. In V2 JSON, Edm.Int64 and Edm.Decimal are serialized as strings (in quotes) instead of numeric JSON values ￼ ￼. This is to avoid JavaScript precision issues (since JavaScript number type cannot precisely represent 64-bit ints or some decimals). Dates (Edm.DateTime) appear as JSON strings in the special "/Date(...)/" format (number of ms since 1970, optionally with offset) ￼. Binary (Edm.Binary) is a base64 string.	OData V4 JSON by default does not require quoting Int64/Decimal – it can represent them as JSON numbers. However, clients can request a safe format using the IEEE754Compatible=true media type parameter if they want large numbers as strings. In practice, many V4 services will output Edm.Int64 as a JSON number (e.g., "Population": 1234567890123) unless that parameter is specified. Edm.Date is output as an ISO date string "YYYY-MM-DD", Edm.DateTimeOffset as an ISO timestamp string, and Edm.Duration as an ISO8601 duration string (e.g., "PT5M"). Essentially, V4 aligns JSON values more closely to their natural representations (with ISO formats) rather than the specialized /Date()/ notation of V2.
Navigation Properties (unexpanded)	In V2 JSON, a navigation property that is not expanded is represented by a JSON object with a __deferred property. For example: "Products": { "__deferred": { "uri": "http://.../Categories(0)/Products" } } indicates a link to related Products was deferred ￼ ￼. The presence of a nav property in the payload (even if deferred) depends on whether it’s selected; by default V2 will include all navigation properties as deferred links in an entry. Clients could navigate them by a separate request.	In V4 JSON minimal, by default navigation properties are omitted entirely if not expanded. The client is expected to know the relationships from the metadata. If a nav prop is expanded, it appears as a normal property with an array or object of expanded entities. If explicitly requested (or in full metadata mode), the service may include a nav link annotation like "Orders@odata.navigationLink": "http://.../Categories(0)/Products" or an "@odata.associationLink" for containment. But in minimal mode, you typically will not see any trace of a nav property that wasn’t expanded, which is a major difference from V2’s always-present deferred links. For implementation, this means your V4 JSON serializer should omit nav properties unless $expand is used, whereas the V2 serializer should output each nav prop with a deferred URI by default ￼ (or at least if that nav prop is included by $select or by default inclusion).
Next Page Links (Server Paging)	If an entity set is partially returned (server-side paging), OData V2 JSON includes an "__next" field (within the "d" object) with the URI to the next page ￼. This is a string URL. It typically appears alongside "results" in the payload.	OData V4 JSON uses "@odata.nextLink" at the top level (outside of the value array) to indicate the next page URL ￼. This is a direct replacement for V2’s __next (no underscores, and with the @odata. prefix to denote an annotation).
Count (Inline Count)	In OData V2, clients can request a total count of the collection by $inlinecount=allpages. The response, when JSON, will include a "__count" property (as a string) inside the d object, alongside the results array ￼. For example: "__count": "57". The count is rendered as a string in JSON (again, likely because it could be large) ￼. If $inlinecount=none, no count is included.	In OData V4, the equivalent is $count=true (as a query option on the request) or using the resource path /Products/$count. With $count=true, the JSON response will include an "@odata.count" property (as a number) in the top-level payload object ￼. If the client specifically fetches /Products/$count, the service returns just the number (as plain text or JSON number with content-type text/plain or application/json depending on implementation). In a normal feed response, "@odata.count": 57 would appear if requested. There is no $inlinecount in V4.

As shown above, the V2 JSON format is heavier, always including the "d" wrapper and lots of __metadata and __... fields, whereas V4’s JSON (especially in minimal metadata mode) is much cleaner. From an implementation perspective, you will likely need two different JSON serialization paths or modes: one that produces the legacy V2 structure and one for the V4 structure. Fortunately, the data content (actual property names and values) will be the same source, but you’ll wrap and annotate it differently.

For example, a simple entity response in V2 JSON might look like:

{
  "d": {
    "__metadata": {
      "uri": "http://server/Service.svc/Products(1)",
      "type": "MyModel.Product",
      "etag": "W/\"08D734\""
    },
    "ID": 1,
    "Name": "Gadget",
    "Category": {
      "__deferred": {
        "uri": "http://server/Service.svc/Products(1)/Category"
      }
    }
  }
}

Whereas the equivalent in V4 JSON minimal could be:

{
  "@odata.context": "http://server/Service.svc/$metadata#Products/$entity",
  "@odata.id": "Products(1)",
  "@odata.etag": "W/\"08D734\"",
  "ID": 1,
  "Name": "Gadget"
  /* no 'Category' property appears since it’s not expanded */
}

Notice the lack of "d" wrapper and __metadata in V4, and that the navigation property Category is omitted entirely. If a client expanded Category, a V4 payload might include "Category": { ... expanded entity ... } directly (or if not fully expanded but selected, potentially an @odata.navigationLink). In V2, expanded nav properties would replace the __deferred object with an array or object under that property name containing the expanded data (with its own __metadata inside each) ￼ ￼.

Summary of JSON format implications: Supporting both formats means your C++ service needs logic to shape query results into two schemas. The content (property names and values coming from DuckDB) can be reused, but the envelope and annotations differ. It may be wise to implement a serializer class for V2 JSON and one for V4 JSON. Also pay attention to numeric formats (V2 requiring certain types as strings) and escaping rules (the /Date(...)\/ format). The official V2 spec defines precisely how to format each Edm type in JSON ￼ ￼, which can guide your implementation. For V4, following OData JSON Format 4.0 guidelines will ensure correctness (e.g., include an @odata.context URI in every response, which points to the relevant portion of the metadata document).

3. JSON Payload Support in OData V2 (Standard vs Extensions)

Despite being an older version, OData V2 does natively support JSON as a first-class format – it’s not just a vendor extension. The OData 2.0 specification included JSON as an alternative to Atom XML for all responses ￼. The goal was to make OData easy to consume in JavaScript and web clients, hence JSON was part of the spec from early on. The official OData 2.0 JSON format documentation spells out how JSON representation works for feeds, entries, properties, etc., and clients can request JSON by setting Accept: application/json or using $format=json ￼ ￼. So you do not need any special extension libraries to output JSON in an OData V2 service – it’s a core feature.

However, it’s worth noting that the JSON produced by a V2-compliant service is the verbose format described above. Later OData versions (OData 3 and some interim implementations) introduced the idea of “JSON Light” (which is essentially what became the V4 minimal metadata format). But in pure OData V2, there is no officially supported “light” JSON – all JSON responses include the d wrapper and metadata by convention. Some servers (e.g. SAP Gateway) historically defaulted to XML and sometimes treated JSON as optional, but according to the spec any OData 2.0 service should support JSON format on request ￼ ￼. In practice, all major OData producers (Microsoft WCF Data Services, SAP OData V2 services, etc.) do support JSON responses (though some clients like the SAP OData V2 adapter expected XML by default, JSON is available via $format=json or proper headers per SAP notes).

In summary, you can confidently implement JSON output for OData V2 as a standard capability, not an extension. Just ensure to follow the OData V2 JSON format rules (especially the security wrapper "d" and the metadata layout) so that generic OData V2 clients (for example, the OData v2 JavaScript library datajs, or older SAP UI5 model v2) can parse it correctly.

4. Metadata and EDMX: Structuring OData V2 $metadata vs V4

When a client requests the service metadata ($metadata endpoint), you need to return an EDMX (Entity Data Model XML) document describing the schema. Both OData V2 and V4 use an XML-based CSDL to describe the model, but the schema format and capabilities have differences corresponding to each version of the EDM.

OData V4 Metadata (EDMX 4.0): In your current implementation, you likely return a metadata document in EDMX format for OData V4. This uses the OData 4.0 CSDL XSD, with the root <Edmx Version="4.0" xmlns="http://docs.oasis-open.org/odata/ns/edmx"> containing an <Edmx:DataServices> section, and inside it one or more <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="..."> elements describing entity types, complex types, entity sets, etc. In OData 4, relationships are expressed inline via navigation properties (no separate <Association> tags). If you have any enum types or type definitions, those are included as <EnumType> and <TypeDefinition> elements (new in V4). V4 metadata can also specify capabilities like containment (NavigationProperty with ContainsTarget="true").

OData V2 Metadata: The OData V2 service metadata is also an XML EDMX document, but adhering to the older CSDL 1.0/2.0 schemas (from Microsoft’s ADO.NET Data Services). The root element might look like <edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx"> (for V2 the EDMX schema is the 2007 Microsoft namespace). Inside, there is an <edmx:DataServices> containing the <Schema xmlns="http://schemas.microsoft.com/ado/2008/09/edm" Namespace="..."> (note the 2008/09 EDM namespace for EDM types in V2). Key structural differences in the content:
	•	Entity Types and Associations: In V2 metadata, each relationship is defined by an <Association Name="X_Y"> element that specifies the two ends (with roles, multiplicities, etc.), and an <AssociationSet> in the EntityContainer tying it to the entity sets. Navigation properties on entity types refer to an Association by name and indicate FromRole/ToRole. This is different from V4 where nav props contain relationship info directly. So to produce V2 metadata, you must transform each navigation property in your model into a corresponding <Association> + <AssociationSet> and use NavigationProperty elements with Relationship="Namespace.AssociationName" Role="End1" attributes ￼.
	•	Type system differences: As discussed, V2 doesn’t have some of the new types. For example, if your DuckDB model has a date column, in V4 metadata you’d use Edm.Date, but in V2 metadata you have to use Edm.DateTime (with perhaps an attribute Precision or Nullable accordingly). Edm.TimeOfDay (V4) would be just Edm.Time in V2 (which represented a time of day or duration in V2 – actually Edm.Time in V2 was essentially a duration measured in days/time). Enums and type definitions simply cannot appear in V2 metadata – if your model includes an enum in V4, you may have to omit that or expose it differently for V2 (or decide not to support that part in V2).
	•	Service Operations/Functions: In V2, function imports are declared in the <EntityContainer> as <FunctionImport Name="X" ReturnType="..." EntitySet="..."> with an attribute m:HttpMethod="GET" if it’s query. V4 has moved some of this to bound/unbound function declarations outside the entity container. If you have any, you’ll need to ensure the V2 metadata lists them in the container (with correct older syntax) so that a V2 client knows about them.
	•	Metadata Annotations: OData V4 allows annotation elements (using <Annotation Term="..." >) for additional semantics, which are part of the OData 4.0 CSDL. V2’s CSDL is more limited; it doesn’t support the OData Vocabulary annotations that V4 does. So if your V4 metadata generation includes any OData annotations (JSON-specific ones or capabilities), those would simply not be present in the V2 $metadata. This likely isn’t a major concern unless you rely on annotations for client behaviors.

Importantly, JSON format metadata is not an option in OData V2. OData V2 clients expect the $metadata at application/xml. There was no JSON representation of the EDMX in OData 2.0. OData V4 (especially in version 4.01 and above) introduced an alternative JSON serialization for CSDL, but even in V4 it’s optional and not commonly used by clients. Typically, even OData V4 clients fetch $metadata as XML EDMX. Your implementation currently provides V4 metadata in XML (which is correct). You should plan to provide a separate EDMX for V2. If you have a single underlying model (DuckDB schema), you might consider writing a converter that can output both a V4 EDMX and a V2 EDMX from the same model definition – taking into account the differences above (especially associations and type name differences). The two metadata documents must describe equivalent data shapes, just in each version’s vocabulary ￼ ￼. For example, the OData Migration library in .NET takes a V3 model and a V4 model and ensures they are semantically identical when serving two versions ￼. You will need to ensure consistency too: the entity sets, keys, properties should match in meaning, even if the syntax differs.

To illustrate the difference, here’s a tiny snippet comparison. Suppose we have two entities, Customer and Order, with a navigation Customer.Orders (1–* many). In OData V4 CSDL it might be:

<Schema Namespace="MyModel" ...>
  <EntityType Name="Customer" Key="ID">
    <Key><PropertyRef Name="ID"/></Key>
    <Property Name="ID" Type="Edm.Int32" Nullable="false"/>
    <NavigationProperty Name="Orders" Type="Collection(MyModel.Order)" Partner="Customer"/>
  </EntityType>
  <EntityType Name="Order" Key="OrderID">
    <Key><PropertyRef Name="OrderID"/></Key>
    <Property Name="OrderID" Type="Edm.Int32" Nullable="false"/>
    <NavigationProperty Name="Customer" Type="MyModel.Customer" Partner="Orders"/>
  </EntityType>
  <EntityContainer Name="Container">
    <EntitySet Name="Customers" EntityType="MyModel.Customer">
      <NavigationPropertyBinding Path="Orders" Target="Orders"/>
    </EntitySet>
    <EntitySet Name="Orders" EntityType="MyModel.Order">
      <NavigationPropertyBinding Path="Customer" Target="Customers"/>
    </EntitySet>
  </EntityContainer>
</Schema>

In OData V2 CSDL, the equivalent relationships would be expressed as:

<Schema Namespace="MyModel" xmlns="http://schemas.microsoft.com/ado/2008/09/edm">
  <EntityType Name="Customer">
    <Key><PropertyRef Name="ID" /></Key>
    <Property Name="ID" Type="Edm.Int32" Nullable="false" />
    <!-- No NavigationProperty here in V2 schema -->
    <NavigationProperty Name="Orders" Relationship="MyModel.Customer_Orders_Association" FromRole="Customer" ToRole="Orders"/>
  </EntityType>
  <EntityType Name="Order">
    <Key><PropertyRef Name="OrderID"/></Key>
    <Property Name="OrderID" Type="Edm.Int32" Nullable="false"/>
    <NavigationProperty Name="Customer" Relationship="MyModel.Customer_Orders_Association" FromRole="Orders" ToRole="Customer"/>
  </EntityType>
  <!-- Association linking Customer and Order -->
  <Association Name="Customer_Orders_Association">
    <End Type="MyModel.Customer" Role="Customer" Multiplicity="1"/>
    <End Type="MyModel.Order" Role="Orders" Multiplicity="*"/>
    <ReferentialConstraint>
      <Principal Role="Customer">
        <PropertyRef Name="ID"/>
      </Principal>
      <Dependent Role="Orders">
        <PropertyRef Name="CustomerID"/> <!-- assuming Order has a CustomerID FK -->
      </Dependent>
    </ReferentialConstraint>
  </Association>
  <EntityContainer Name="Container">
    <EntitySet Name="Customers" EntityType="MyModel.Customer"/>
    <EntitySet Name="Orders" EntityType="MyModel.Order"/>
    <AssociationSet Name="Customer_Orders_AssociationSet" Association="MyModel.Customer_Orders_Association">
      <End Role="Customer" EntitySet="Customers"/>
      <End Role="Orders" EntitySet="Orders"/>
    </AssociationSet>
  </EntityContainer>
</Schema>

As you can see, the V2 $metadata is more verbose and structured differently. Your system will need to produce something like this for V2. JSON metadata for V2 is not available – a request like $metadata?$format=json is not defined in OData 2.0 (and should return an error or be ignored). Even in V4, JSON metadata would only be used if a client specifically requests it (e.g., Accept: application/json on $metadata or $metadata?$format=json in OData 4.01 services) ￼. Typically, plan on serving XML EDMX for both V2 and V4 $metadata, with the respective schemas.

One more note: OData version headers interplay with metadata as well. In OData V2, the DataServiceVersion header in a $metadata response might be 2.0; (meaning this metadata is up to v2). In OData V4, the OData-Version: 4.0 header would be returned. Keep an eye on sending the correct version headers with the metadata responses.

5. Compatibility Considerations for Concurrent V2 and V4 Support

Supporting both OData V2 and V4 in the same service (or at least on the same backend) raises questions of content negotiation, URI structures, and query option differences. Here are the main considerations and how to handle them:

5.1 Service Root and Endpoint Structure

One decision is how you expose the two versions: separate endpoints vs. single endpoint with negotiation. Many implementations choose to have distinct base URLs for different OData versions (for example, /odata/v2/Service.svc/... and /odata/v4/Service.svc/...). This cleanly separates the metadata and payload formats. If you go this route, the “service root” URI for each version will be different (as part of the URL path). In that case, content negotiation is simpler (each endpoint only speaks one OData version). The downside is the need to maintain two endpoints.

If instead you try to use one endpoint that can speak both, you must rely on OData version headers in requests to decide the format. OData V2 clients send DataServiceVersion and MaxDataServiceVersion headers in their requests (and expect them in responses), whereas OData V4 clients use the OData-Version header. For example, a V2 client request might include:

DataServiceVersion: 2.0  
MaxDataServiceVersion: 2.0  

A V4 client request would typically have:

OData-Version: 4.0  
OData-MaxVersion: 4.0  

Your server can inspect these to decide which version of the protocol to use for the response ￼. If no version headers are present, by spec you should default to the oldest version for safety (V2 in this case) ￼ ￼, but you might also use heuristics (e.g., if the request Accept header has odata.metadata=minimal or other V4-specific constructs, you can assume V4). This is complex and can be brittle. Practically, using distinct endpoints or requiring clients to specify the version via the URL (or a query param) is easier.

Service Document Differences: The service document (the response from a GET to the service root URL) also differs between V2 and V4. In OData V2, the service document is a list of entity set names in either XML or JSON. The JSON format for a V2 service document is: {"d": { "EntitySets": ["Products","Orders", ...] } } ￼ ￼ – just an array of names. In OData V4, the service document JSON contains an array of objects, each with at least "name" and "url", and a "kind" (like EntitySet, Singleton, FunctionImport, etc.) if not an entity set. For example: {"@odata.context":"...$metadata", "value":[ {"name":"Products","kind":"EntitySet","url":"Products"}, ... ] }. If you share an endpoint, you’d need to generate the appropriate service document depending on version. With separate endpoints, you can hardcode one to return the V2 style and the other the V4 style.

URI Conventions: By and large, resource path conventions are similar between V2 and V4, but there are some differences to keep in mind:
	•	Entity key syntax: Both use EntitySet(key) for single keys. For multiple-key entities, V2 required the keys to be named in the parentheses (e.g. Orders(OrderID=1, CustomerID=2)), whereas V4 also allows that (named-key syntax is still allowed) but if keys have a defined ordering, V4 allows just EntitySet(Key1,Key2) without property names. It’s a minor difference; supporting the named syntax in both is fine. Also, as noted, V2 expected GUID keys in the form guid'XXXXXXXX-XXXX-...', whereas V4 expects GUID literals without the guid' prefix (just as a 36-char string literal) ￼. The Migration example shows that an OData V3 URL Product(guid'...') would become Product(... ) without the prefix in V4 ￼. So if you get a V2 request with guid'...' in the URL, your parser should accept it and strip the guid prefix for actual querying. Similarly, V4 filter syntax for GUID might not include the prefix.
	•	Count and value paths: In V2, to get the raw value of a primitive property or an open property, one would just GET that property and by convention it returns the value (possibly as text/plain). V3 introduced /property/$value. In V2, $value was more commonly used for media resources (stream content). OData V4 formalized the $value segment for any primitive or stream property. You might consider implementing /EntitySet(key)/Property/$value for V4, and possibly also accept it for V2 for consistency (though V2 clients might instead do things like look at the property in the entry). Likewise, /$count on collections exists in V2 for navigation collections (the V2 spec allowed e.g. ../Orders/$count to return the count as plain text) ￼, but V2 clients more commonly used $inlinecount. In V4, /Orders/$count returns count (as plain text or JSON number), and $count=true in queries to include it. Ensure that if a V2 client calls /$count you return a plain number text and not a JSON object, since V2 expects text count (the V2 spec says the response to a $count request is just the number, content-type text/plain). A V4 client calling the same might accept either, but following V4 spec you could actually return it with text/plain as well. Just be mindful of these nuances if implementing count.

5.2 System Query Options and Parameters

$format: Both V2 and V4 support the $format query option to explicitly request a format, which overrides the Accept header ￼ ￼. In V2, valid $format values included json and atom (and xml for raw xml) ￼. In V4, $format can accept json, but also things like application/json;odata.metadata=full etc. For your implementation, $format=json could be accepted on both version endpoints, but it should trigger the appropriate JSON format for that version. If someone tries $format=atom on the V2 endpoint, since you don’t plan to support Atom, you should return 406 Not Acceptable or a clear error indicating only JSON is supported (or simply ignore $format and still return JSON if you prefer). On the V4 endpoint, $format=atom isn’t valid at all (Atom isn’t supported in V4 spec). So you may want to explicitly handle or reject unsupported format requests to avoid confusion.

$select and $expand: Both versions support $select and $expand for shaping the data, but V4 has more advanced capabilities. In OData V2, you can $expand relations and $select certain properties, but you cannot apply nested options within an $expand. For example, V2 could do ?$expand=Orders&$select=Name,Orders (which brings in all Orders with all their properties by default, since you can’t select within Orders in that query). OData V4 allows more fine-grained control: you can expand with nested $select, $filter, $orderby, $top, etc. (e.g., ?$expand=Orders($select=OrderDate;$filter=Amount gt 100) to only include certain fields of Orders and filtered Orders) ￼. If your implementation already supports these advanced facets for V4, you should ensure that on the V2 endpoint, if a client attempts to use any V4-only syntax, you reject it as bad request. Typical V2 clients won’t send those, but a misinformed client might. The SAP comparison notes that V4 supports applying system queries on expanded entities, whereas V2 only supports them at the root level ￼ ￼.

Also, $select behavior difference: In V2, if you $select some properties, the payload will include only those properties and, by convention, still the __metadata (and any deferred nav links for selected navs). In V4, if you $select, the payload includes only those properties and needed annotations. Both behave fairly analogously here, but note: If a nav prop is not expanded, including it in $select in V4 is a bit odd (since by default it wouldn’t appear). In V4 minimal, $select=NavProp with no expand likely does nothing (the service might omit it, or the spec might allow an @odata.navigationLink to appear if metadata=full). In V2, $select=NavProp would include the deferred link object for that nav. So if you need to handle $select on nav props: for V2, include the __deferred link for that nav; for V4, possibly ignore or include a nav link annotation. Most V4 clients would use $expand if they actually want the nav content.

$filter and Query Functions: The basic operators ($filter with eq, ne, gt, etc.) are the same in V2 and V4. However, the set of functions available in $filter differs. OData V2 (and V3) used functions like substringof(substring, property) and startswith(property, substring), endswith(property, substring) ￼. OData V4 introduced more standard ones: contains(property, substring) (replacing substringof’s semantics) and still uses startswith and endswith but the parameter order might be reversed (need to check: V4 defines contains(x,y), startswith(x,y), endswith(x,y) consistently). For example, a V2 client might send $filter=substringof('Alfreds', CompanyName) eq true ￼, while a V4 client would do $filter=contains(CompanyName,'Alfreds') eq true (or simply contains(CompanyName,'Alfreds') since it returns boolean). Your query parser needs to recognize both and map them appropriately to DuckDB SQL (perhaps both can translate to a SQL LIKE or POSITION function). Similarly, date functions or math functions might differ slightly. OData V4 added lambda operators (any, all) for filtering on collections; V2 does not have those. If a V2 client tries to use any/all, it’s not valid – likely won’t happen unless they are actually a V4 client. Conversely, if you have a complex filter that uses an OData 4 feature, on the V2 endpoint that should be rejected or unsupported.

$orderby, $top, $skip: These work the same in both versions. One nuance: in V2, $skiptoken was used for server paging continuation (the value from __next query param), whereas V4’s nextLink encapsulates the skip token. You as a server can generate continuation tokens for paging however you like; just ensure the V2 next link is formed as ...?$skiptoken=XYZ (that’s what the V2 convention example shows ￼) whereas a V4 nextLink might include $skiptoken or $skip internally – essentially it’s the same URL. So actually, you can use the same mechanism to generate the next page URL, just output it in the correct JSON field.

$inlinecount vs $count: As covered, these differ. If you detect $inlinecount in a query on the V4 endpoint, you should reject it (not supported in V4). If $count=true on a V2 endpoint, you might ignore or reject it since V2 expects $inlinecount. Ideally, each endpoint only advertises/accepts the appropriate ones. So your router or parser could be version-aware: in V2 mode, interpret $inlinecount and inject a count in response; in V4 mode, look for $count=true or the resource path ending in /$count.

Format of count: If implementing inline count in V2 JSON, remember it must be a string value in JSON (even though it’s numeric) ￼. In V4 JSON, count is a number without quotes. This is a small but important difference in the JSON output.

IEEE754 compatibility: This is a minor content negotiation thing. OData V4 clients concerned about large numbers can add ;IEEE754Compatible=true to their Accept header or $format. OData V2 has no concept of this parameter – it always treated 64-bit numbers as strings. If you decide to honor IEEE754Compatible for V4, that’s fine; on V2, any such parameter in Accept can be ignored safely or cause no change (since you already output as strings). This mostly affects how you format Int64 and Decimal in V4 JSON.

Content-Type and Accept Headers: For V2 JSON responses, you will use Content-Type: application/json;odata=verbose;charset=utf-8 in Microsoft implementations. The official spec didn’t define a special parameter for verbose JSON – it was just application/json. In practice, some clients (like older DataService clients) might send Accept: application/json;odata=verbose to explicitly request the old format. OData V4 uses application/json;odata.metadata=minimal (or full/none) in its responses and in content negotiation. Be prepared to handle these. If you see Accept: application/json;odata.metadata= in a request, that is definitely a V4 client. If you see Accept: application/json;odata=verbose, it’s a V2 client (or an OData 3 JSON client, which was similar to V2’s format). So these Accept parameters can guide version choice. When sending responses, follow the version’s convention:
	•	V2 response: Content-Type: application/json (optionally ;odata=verbose – not strictly necessary to include, but some implementations do). The DataServiceVersion header in the response should be 2.0; to indicate the payload is V2 format.
	•	V4 response: Content-Type: application/json;odata.metadata=minimal;charset=UTF-8 (or whatever metadata level was requested). And include OData-Version: 4.0 header.

Default behavior if no Accept header: According to OData 2 spec, default to Atom/XML ￼. In OData 4, default is JSON (since Atom not primary). If you have separate endpoints, you might enforce that the v2 endpoint still returns JSON by default (contrary to spec default) since you’re not supporting Atom. That’s fine – just document that your service only produces JSON. If using one endpoint, you might want to assume JSON as default to avoid someone accidentally getting an XML they didn’t expect.

5.3 Handling Clients and Compatibility Pitfalls

Because OData V2 clients and V4 clients are different, test with at least one of each type if possible:
	•	For OData V2, there are libraries like the SAP UI5 OData V2 model, or the older Microsoft Data Services client, that expect the exact verbose JSON structure. They might also expect certain behaviors (like the presence of __metadata, or certain casing of fields exactly as specified). Ensure your JSON format is precisely as per spec (the examples from the official docs are a good guide).
	•	Check that the $metadata for V2 can be consumed by an OData V2 codegen or viewer (for example, use a tool like odata2poco or a SuccessFactors EDMX viewer). Misplacing an Association or role name could confuse a client.
	•	For OData V4, use a modern OData client (like ODataConnectedService in .NET or a JavaScript OData client for v4) to see that your $metadata and responses are acceptable.

One tricky area is if you try to reuse the same model and there are slight differences – e.g., Edm.Date in V4 vs Edm.DateTime in V2. A client might, for example, filter on a date property. An OData V4 client would write $filter=MyDate eq 2025-08-07 (with the literal interpreted as Edm.Date automatically). An OData V2 client might write $filter=MyDate eq datetime'2025-08-07T00:00:00'. Your implementation’s filter parser should be ready for both syntaxes. You might need to implement the literal parsing differences (the OData V2 URI conventions define datetime'...' and guid'...' etc. as literal notations ￼, whereas V4 uses ISO literals or simple numeric/string representations). If you have a unified parsing logic, make it version-aware.

5.4 Concurrency and ETags

This is read-only so you might not care about updates, but note that if you include ETags in your response (for caching or concurrency tokens), V2 and V4 handle them slightly differently in JSON:
	•	V2: ETag is given in the __metadata: { etag: "value" } for each entry ￼.
	•	V4: ETag is an annotation @odata.etag on the entry ￼.
This is just another format detail to implement, but mentioning it for completeness. The handling of If-None-Match or If-Match on GET requests would be similar in both versions (HTTP-level), but if you ever needed to send back a 304 Not Modified, ensure you handle the version headers accordingly.

6. References and Tools for Dual-Version Support (Optional)

Implementing both versions in one system is non-trivial, but there are reference points in the community:
	•	Microsoft’s OData Migration Library: This is an ASP.NET Core library that allows an OData V4 service to also handle V3 requests by translating on the fly ￼. It essentially takes an incoming V3 URI/headers, translates them to V4 internally, and then converts the response back to V3 format (JSON). While it targets V3, many concepts apply to V2 since V3’s JSON was similar to V2’s. Notably, it intercepts the DataServiceVersion headers and uses separate EDM models for each version ￼ ￼. This shows an approach where a single codebase can respond as older OData by keeping parallel metadata models. For V2/V4, you could adopt a similar strategy: maintain one EDM model for V4 and one for V2 (derived from the same source), and on each request choose the appropriate one and formatting.
	•	Apache Olingo: Apache Olingo is an open-source Java library for OData. It has separate modules for V2 (org.apache.olingo.odata2) and V4 (org.apache.olingo.odata4). While not a single library supporting both simultaneously, a single application could theoretically host both a V2 and V4 endpoint using Olingo. Studying Olingo’s approach might give insight into the differences. For example, Olingo V2 module implements the JSON serialization per V2 spec, which you can mimic.
	•	SAP Gateway and CAP: SAP’s older Gateway supported only OData V2, but SAP’s newer Cloud Application Programming model (CAP) by default produces OData V4. They have an “V2 adapter” that can on-the-fly downgrade an OData V4 service to OData V2 for compatibility (mostly used to support legacy UI5 apps that expect V2). This adapter translates metadata and payloads, similar to Microsoft’s migration library. It’s not open-source, but its existence confirms that maintaining a single source model and dual outputs is feasible.
	•	Progress DataDirect’s Hybrid Data Pipeline: as mentioned in a blog, this product can expose both OData 2.0 and 4.0 endpoints over the same data ￼. The blog “OData 2 vs OData 4” by Progress highlights some differences and implies their server supports both simultaneously. This is a commercial example but it demonstrates the concept.

In C++, there isn’t an off-the-shelf library doing OData V2/V4, so your implementation (erpl-web) will likely handle it manually. One way to structure it: at runtime, have a mode or flag in the request context for ODataVersion. All your query parsing, metadata rendering, and response formatting functions can take this into account (or you maintain separate code paths where it diverges significantly). For instance, you might have WriteJsonEntryV2() and WriteJsonEntryV4() functions, or a strategy pattern for JSON serialization.

Testing tools: You can use the public OData V2 service (e.g. OData.org’s Northwind V2 service) and V4 service (TripPin, etc.) to compare outputs for similar queries. This will give you a feel for what clients expect. Additionally, some open source projects can consume both: for example, some OData client libraries (like Simple.OData.Client in .NET) can be toggled between V2 and V4, which might be useful for validating your service outputs.

Finally, keep in mind that OData V2 is considered legacy (even OData.org suggests using V4) and some newer clients might not support it. But if your user base includes older tools or requires V2 (perhaps due to some integration or UI framework), these efforts will ensure compatibility.

References:
	•	OData 2.0 JSON Format specification ￼ ￼ (structure of V2 JSON payloads, showing the "d" wrapper, "results", __metadata, etc.).
	•	OData 4.0 Protocol and JSON Format (OASIS specs and OData.org tutorials) ￼ ￼ (examples of V4 JSON payload with @odata.context, value, etc., and optional metadata control).
	•	SAP OData V2 vs V4 comparison ￼ ￼ (highlights differences in metadata format support and data types between the versions).
	•	Progress Software blog on OData 2 vs 4 ￼ ￼ (confirms that V2 required XML+JSON support, V4 is JSON-first, and notes improvements in $expand and other query capabilities in V4).
	•	Microsoft OData Migration library docs ￼ ￼ (demonstrates handling multiple OData versions concurrently by detecting version headers and translating models).