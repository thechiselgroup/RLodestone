#' @export
startRLodestone <- function(port = 3010) {
	svSocket::stopSocketServer(port)
	svSocket::startSocketServer(port, procfun = processMsg)	
}

#' @export
stopRLodestone <- function(port = 3010) {
	svSocket::stopSocketServer(port)	
}

#' @export
addIdColumn <- function(df, idField, varName) {
	if (is.null(idField)) {
		df$'_id' <- paste0('RSession', varName, seq.int(nrow(df)))
	} else {
		df$'_id' <- df$`options$idField`
	}
	df
}

getData <- function(varName, options) {
	if (!exists(varName)) {
		msg <- paste0('The variable "', varName, '" is not defined.' )
		return(list(error=jsonlite::unbox(msg)))
	}
	
	data <- get(varName)
	
	data <- addIdColumn(data, options$idField, varName)
	
	# filtering per id has to be done first, otherwise, if the only included field is _id, we will end up with a vector
	if (!is.null(options$matchIds)) {
		data <- data[data$'_id' %in% options$matchIds, ]
	}

	if (!is.null(options$returnCount) && options$returnCount) {
		return(nrow(data))
	}
	
	if(!is.null(options$includedFields) && length(options$includedFields) > 0) {
		data <- data[unique(c('_id', unlist(options$includedFields)))]
	}
	
	if (!is.null(options$limit) && options$limit != 0) {
		data <- data[1:options$limit,, drop=F]
	}
	
	return(data)
}

applyStatements <- function(statements){
	statements <<- statements
	for (statement in statements) {
		eval(parse(text=statement))
	}
}

processMsg <- function(msg, socket, port) {
	jsonMsg <- jsonlite::fromJSON(msg)
	message(paste('Processing method', jsonMsg$method))
	if (jsonMsg$method == 'getData') {
		data <- getData(jsonMsg$varName, jsonMsg$options)
		stringData <- paste0(jsonlite::toJSON(data), '\n')
		
		return(stringData)
	}
	
	if (jsonMsg$method == 'applyStatements') {
		applyStatements(jsonMsg$statements)
		return(0)
	}
}
